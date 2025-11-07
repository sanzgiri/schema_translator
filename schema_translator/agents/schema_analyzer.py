"""
Schema Analyzer Agent: Automatically analyze customer schemas and propose mappings.
"""

import json
from typing import List, Dict, Any, Tuple
from anthropic import Anthropic
from ..models import (
    CustomerSchema,
    SchemaTable,
    SchemaColumn,
    SemanticType,
    ConceptMapping,
)
from ..knowledge_graph import SchemaKnowledgeGraph


class SchemaAnalyzerAgent:
    """
    Automatically analyzes new customer database schemas and proposes
    semantic concept mappings using LLM-powered understanding.
    """

    def __init__(self, anthropic_api_key: str, knowledge_graph: SchemaKnowledgeGraph):
        """
        Initialize the schema analyzer agent.

        Args:
            anthropic_api_key: API key for Anthropic Claude
            knowledge_graph: Schema knowledge graph with semantic concepts
        """
        self.client = Anthropic(api_key=anthropic_api_key)
        self.kg = knowledge_graph
        self.model = "claude-sonnet-4-20250514"

    def _build_system_prompt(self) -> str:
        """Build the system prompt with available semantic concepts."""
        concepts = []
        for concept_name, concept_data in self.kg.concepts.items():
            description = concept_data.description if hasattr(concept_data, 'description') else 'No description'
            concepts.append(
                f"- {concept_name}: {description}"
            )
        concept_list = "\n".join(concepts)

        return f"""You are a database schema analysis assistant. Your job is to analyze customer database schemas and identify which columns map to semantic concepts.

Available Semantic Concepts:
{concept_list}

Your task is to:
1. Examine the customer's table and column names
2. Consider column data types and constraints
3. Identify which columns represent which semantic concepts
4. Determine if any transformations are needed (e.g., converting annual to lifetime values)
5. Provide confidence scores (0.0 to 1.0) for each mapping

Semantic Types:
- "string": Text values (VARCHAR, TEXT, CHAR)
- "number": Numeric values (INT, FLOAT, DECIMAL, BIGINT)
- "date": Date values (DATE, DATETIME, TIMESTAMP)
- "boolean": True/False values (BOOLEAN, TINYINT(1))
- "enum": One of a fixed set of values

Return ONLY valid JSON matching this schema:
{{
    "mappings": [
        {{
            "concept": "semantic_concept_name",
            "table": "table_name",
            "column": "column_name",
            "confidence": 0.95,
            "reasoning": "Why this mapping makes sense",
            "transformation": "Optional: transformation rule if needed"
        }}
    ]
}}"""

    def _build_user_prompt(
        self, customer_id: str, schema: CustomerSchema
    ) -> str:
        """Build the user prompt with schema details."""
        # Format schema information
        schema_text = f"Customer: {customer_id}\n\nTables and Columns:\n"
        for table in schema.tables:
            schema_text += f"\nTable: {table.name}\n"
            for column in table.columns:
                constraints = []
                if column.is_primary_key:
                    constraints.append("PRIMARY KEY")
                if column.is_foreign_key:
                    constraints.append("FOREIGN KEY")

                constraint_str = ", ".join(constraints) if constraints else ""
                schema_text += f"  - {column.name} ({column.data_type}) {constraint_str}\n"

        return f"""{schema_text}

Analyze this schema and identify mappings to semantic concepts.

Example mappings for reference:

Customer A:
- contract_identifier → contracts.contract_id (confidence: 1.0, exact name match)
- contract_value → contracts.total_value (confidence: 0.95, "total_value" represents contract value)
- contract_expiration → contracts.end_date (confidence: 0.90, "end_date" is when contract expires)

Customer B (multi-table):
- customer_name → customers.customer_name (confidence: 1.0, requires JOIN via contracts.customer_id)

Customer D (transformation needed):
- contract_expiration → agreements.days_remaining (confidence: 0.85, transformation: "Convert days_remaining to actual date using CURRENT_DATE")

Consider:
1. Exact name matches have highest confidence
2. Semantic equivalents (e.g., "end_date" for expiration) have high confidence
3. Multi-table mappings may require JOINs
4. Some fields may need transformations (e.g., days → date, annual → lifetime)

Return ONLY the JSON object with mappings."""

    def analyze_schema(
        self, customer_id: str, schema: CustomerSchema, max_retries: int = 2
    ) -> List[ConceptMapping]:
        """
        Analyze a customer schema and propose concept mappings.

        Args:
            customer_id: The customer identifier
            schema: The customer's database schema
            max_retries: Maximum number of retries on parsing errors

        Returns:
            List of ConceptMapping objects with proposed mappings

        Raises:
            ValueError: If unable to analyze schema after retries
        """
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(customer_id, schema)

        for attempt in range(max_retries + 1):
            try:
                # Call Claude API
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                    temperature=0.0,  # Deterministic for structured output
                )

                # Extract JSON from response
                response_text = message.content[0].text.strip()

                # Handle markdown code blocks
                if response_text.startswith("```"):
                    lines = response_text.split("\n")
                    response_text = "\n".join(
                        line for line in lines if not line.startswith("```")
                    )

                # Parse JSON
                result = json.loads(response_text)

                # Convert to ConceptMapping objects
                mappings = []
                for m in result["mappings"]:
                    # Find the actual column to get semantic type and data type
                    semantic_type = SemanticType.TEXT  # Default
                    data_type = "TEXT"  # Default
                    for table in schema.tables:
                        if table.name == m["table"]:
                            for column in table.columns:
                                if column.name == m["column"]:
                                    data_type = column.data_type
                                    # Infer semantic type from SQL type
                                    sql_type = column.data_type.upper()
                                    if any(
                                        t in sql_type
                                        for t in ["INT", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC", "REAL"]
                                    ):
                                        semantic_type = SemanticType.FLOAT
                                    elif any(
                                        t in sql_type for t in ["DATE", "TIME", "TIMESTAMP"]
                                    ):
                                        semantic_type = SemanticType.DATE
                                    elif any(
                                        t in sql_type for t in ["BOOL", "TINYINT(1)"]
                                    ):
                                        semantic_type = SemanticType.BOOLEAN
                                    else:
                                        semantic_type = SemanticType.TEXT
                                    break

                    mapping = ConceptMapping(
                        customer_id=customer_id,
                        table_name=m["table"],
                        column_name=m["column"],
                        data_type=data_type,
                        semantic_type=semantic_type,
                        transformation=m.get("transformation"),
                    )
                    # Store additional metadata from LLM (not in model)
                    mapping._confidence = m.get("confidence", 0.8)
                    mapping._reasoning = m.get("reasoning", "")
                    mapping._concept = m["concept"]
                    mappings.append(mapping)

                # Validate that all concepts exist
                for mapping in mappings:
                    if mapping._concept not in self.kg.concepts:
                        raise ValueError(
                            f"Unknown semantic concept: {mapping._concept}. "
                            f"Available: {list(self.kg.concepts.keys())}"
                        )

                return mappings

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                if attempt < max_retries:
                    user_prompt += f"\n\nPrevious attempt failed: {str(e)}\nPlease try again with valid JSON."
                    continue
                else:
                    raise ValueError(
                        f"Failed to analyze schema after {max_retries + 1} attempts. "
                        f"Last error: {str(e)}"
                    )

        raise ValueError("Unexpected error in schema analysis")

    def explain_mappings(
        self, mappings: List[ConceptMapping], include_low_confidence: bool = False
    ) -> str:
        """
        Generate a human-readable explanation of proposed mappings.

        Args:
            mappings: List of concept mappings
            include_low_confidence: Whether to include low-confidence mappings

        Returns:
            Human-readable explanation
        """
        lines = ["Proposed Schema Mappings:\n"]

        # Group by concept
        by_concept: Dict[str, List[ConceptMapping]] = {}
        for mapping in mappings:
            confidence = getattr(mapping, '_confidence', 1.0)
            concept = getattr(mapping, '_concept', 'unknown')
            if not include_low_confidence and confidence < 0.5:
                continue
            if concept not in by_concept:
                by_concept[concept] = []
            by_concept[concept].append(mapping)

        for concept, concept_mappings in sorted(by_concept.items()):
            concept_display = concept.replace("_", " ").title()
            lines.append(f"\n{concept_display}:")
            for mapping in concept_mappings:
                confidence = getattr(mapping, '_confidence', 1.0)
                confidence_pct = int(confidence * 100)
                location = f"{mapping.table_name}.{mapping.column_name}"
                line = f"  - {location} ({confidence_pct}% confidence)"
                if mapping.transformation:
                    line += f"\n    Transformation: {mapping.transformation}"
                lines.append(line)

        return "\n".join(lines)

    def validate_mappings(
        self,
        customer_id: str,
        schema: CustomerSchema,
        mappings: List[ConceptMapping],
    ) -> Tuple[List[ConceptMapping], List[str]]:
        """
        Validate proposed mappings against the actual schema.

        Args:
            customer_id: The customer identifier
            schema: The customer's database schema
            mappings: Proposed concept mappings

        Returns:
            Tuple of (valid_mappings, error_messages)
        """
        valid_mappings = []
        errors = []

        # Build a lookup of available tables and columns
        schema_lookup = {}
        for table in schema.tables:
            schema_lookup[table.name] = {
                col.name for col in table.columns
            }

        for mapping in mappings:
            # Check if table exists
            if mapping.table_name not in schema_lookup:
                errors.append(
                    f"Table '{mapping.table_name}' not found in schema for {customer_id}"
                )
                continue

            # Check if column exists
            if mapping.column_name not in schema_lookup[mapping.table_name]:
                errors.append(
                    f"Column '{mapping.column_name}' not found in table "
                    f"'{mapping.table_name}' for {customer_id}"
                )
                continue

            # Check if concept exists (stored as _concept attribute)
            concept = getattr(mapping, '_concept', None)
            if concept and concept not in self.kg.concepts:
                errors.append(f"Unknown concept: {concept}")
                continue

            # Mapping is valid
            valid_mappings.append(mapping)

        return valid_mappings, errors
