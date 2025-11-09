"""
Query Understanding Agent: Parse natural language queries into semantic query plans.
"""

import json
from typing import Optional
from anthropic import Anthropic
from ..models import (
    SemanticQueryPlan,
    QueryFilter,
    SemanticType,
    QueryOperator,
    QueryIntent,
)
from ..knowledge_graph import SchemaKnowledgeGraph
from ..config import Config


class QueryUnderstandingAgent:
    """
    Translates natural language queries into structured SemanticQueryPlan objects
    using LLM-powered semantic understanding.
    """

    def __init__(
        self, 
        anthropic_api_key: str, 
        knowledge_graph: SchemaKnowledgeGraph,
        config: Optional[Config] = None
    ):
        """
        Initialize the query understanding agent.

        Args:
            anthropic_api_key: API key for Anthropic Claude
            knowledge_graph: Schema knowledge graph for semantic concepts
            config: Configuration object (creates new if None)
        """
        self.client = Anthropic(api_key=anthropic_api_key)
        self.kg = knowledge_graph
        self.config = config or Config()
        self.model = self.config.model_name
        self.max_tokens = self.config.max_tokens
        self.temperature = self.config.temperature

    def _build_system_prompt(self) -> str:
        """Build the system prompt with available semantic concepts."""
        concepts = list(self.kg.concepts.keys())
        concept_list = "\n".join([f"- {concept}" for concept in concepts])

        return f"""You are a semantic query understanding assistant. Your job is to parse natural language queries about contracts into structured semantic query plans.

Available Semantic Concepts:
{concept_list}

CRITICAL RULE - Database Selection:
When users mention "customer" or database references, extract them into target_customers field:
- "customer_a", "customer_b", "customer_c", "customer_d", "customer_e", "customer_f"
- "customer A", "customer B", "database A", "from customer_a", "customer a database"
- Any reference to which specific database(s) to query

DO NOT create semantic filters for these - use the target_customers field instead.
Examples:
- "Show contracts from customer_a" → target_customers: ["customer_a"]
- "Query customer A and B" → target_customers: ["customer_a", "customer_b"]  
- "Show all contracts" → target_customers: null (means query all)

Your task is to:
1. Identify the user's query intent (find_contracts, count_contracts, aggregate_values)
2. Determine which semantic concepts are relevant
3. Extract any filter conditions with proper operators and values
4. Return a valid JSON object matching the SemanticQueryPlan schema

Query Intent Types:
- "find_contracts": User wants to find/list specific contracts
- "count_contracts": User wants to count contracts
- "aggregate_values": User wants statistics (sum, average, etc.)

Query Operators:
- "equals": Exact match (=)
- "not_equals": Not equal (!=)
- "greater_than": Greater than (>)
- "less_than": Less than (<)
- "greater_than_or_equal": Greater than or equal (>=)
- "less_than_or_equal": Less than or equal (<=)
- "contains": String contains
- "in": Value in list
- "between": Value between two bounds

Semantic Types (use these exact values):
- "text": Text values
- "integer": Integer numbers
- "float": Floating point numbers
- "date": Date values (YYYY-MM-DD format)
- "boolean": True/False values

Return ONLY valid JSON matching this schema:
{{
    "intent": "find_contracts" | "count_contracts" | "aggregate_values",
    "filters": [
        {{
            "concept": "concept_name",
            "operator": "operator_name",
            "value": "value" | number | ["list"],
            "semantic_type": "text" | "integer" | "float" | "date" | "boolean"
        }}
    ],
    "projections": ["concept1", "concept2"],  // List specific fields, or [] for ALL fields
    "aggregations": [{{
        "function": "count" | "sum" | "average",
        "concept": "concept_name"
    }}],  // optional, for aggregate_values intent
    "limit": 10,  // optional, for find_contracts intent
    "target_customers": ["customer_a", "customer_b"]  // optional, null or omit for all customers
}}

IMPORTANT: 
- If user asks for "all contracts", "show me contracts", or doesn't specify which fields, use EMPTY projections list []
- Empty projections [] means return ALL available fields
- Only specify projections when user explicitly asks for specific fields"""

    def _build_user_prompt(self, natural_language_query: str) -> str:
        """Build the user prompt with examples."""
        return f"""Parse this natural language query into a semantic query plan:

"{natural_language_query}"

Examples:

Query: "Show me all contracts" or "List all contracts"
Result:
{{
    "intent": "find_contracts",
    "filters": [],
    "projections": [],  // Empty projections means return ALL available fields
    "limit": 100,
    "target_customers": null  // null means query all customers
}}

Query: "Show me all active contracts"
Result:
{{
    "intent": "find_contracts",
    "filters": [
        {{
            "concept": "contract_status",
            "operator": "equals",
            "value": "active"
        }}
    ],
    "projections": [],  // Empty projections means return ALL available fields
    "limit": 100,
    "target_customers": null
}}

Query: "Show contracts from customer_a" or "Query customer A database"
Result:
{{
    "intent": "find_contracts",
    "filters": [],
    "projections": [],
    "limit": 100,
    "target_customers": ["customer_a"]  // Extract database reference
}}

Query: "How many contracts expire in the next 90 days?"
Result:
{{
    "intent": "count_contracts",
    "filters": [
        {{
            "concept": "contract_expiration",
            "operator": "between",
            "value": ["TODAY", "TODAY+90"]
        }}
    ],
    "aggregations": [{{
        "function": "count",
        "concept": "contract_identifier"
    }}]
}}

Query: "Find contracts worth more than 2 million dollars"
Result:
{{
    "intent": "find_contracts",
    "filters": [
        {{
            "concept": "contract_value",
            "operator": "greater_than",
            "value": 2000000
        }}
    ],
    "projections": ["contract_identifier", "contract_value", "contract_status"]
}}

Query: "List contracts expiring in 2026"
Result:
{{
    "intent": "find_contracts",
    "filters": [
        {{
            "concept": "contract_expiration",
            "operator": "between",
            "value": ["2026-01-01", "2026-12-31"]
        }}
    ],
    "projections": ["contract_identifier", "contract_expiration", "contract_value"],
    "limit": 50
}}

Now parse the user's query and return ONLY the JSON object."""

    def understand_query(
        self, natural_language_query: str, max_retries: int = 2
    ) -> SemanticQueryPlan:
        """
        Parse a natural language query into a SemanticQueryPlan.

        Args:
            natural_language_query: The user's natural language query
            max_retries: Maximum number of retries on parsing errors

        Returns:
            SemanticQueryPlan object

        Raises:
            ValueError: If unable to parse query after retries
        """
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(natural_language_query)

        for attempt in range(max_retries + 1):
            try:
                # Call Claude API
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                    temperature=self.temperature,
                )

                # Extract JSON from response
                response_text = message.content[0].text.strip()

                # Handle markdown code blocks
                if response_text.startswith("```"):
                    # Remove markdown code fence
                    lines = response_text.split("\n")
                    response_text = "\n".join(
                        line for line in lines if not line.startswith("```")
                    )

                # Parse JSON
                plan_dict = json.loads(response_text)

                # Convert to Pydantic model
                # First, convert filter dicts to QueryFilter objects
                if "filters" in plan_dict:
                    filters = []
                    for f in plan_dict["filters"]:
                        filters.append(
                            QueryFilter(
                                concept=f["concept"],
                                operator=QueryOperator(f["operator"]),
                                value=f["value"],
                            )
                        )
                    plan_dict["filters"] = filters

                # Convert aggregations if present
                if "aggregations" in plan_dict and plan_dict["aggregations"]:
                    from ..models import QueryAggregation
                    aggs = []
                    for agg in plan_dict["aggregations"]:
                        if isinstance(agg, str):
                            # Old format: just function name
                            aggs.append(QueryAggregation(
                                function=agg,
                                concept="contract_identifier"  # default
                            ))
                        else:
                            # New format: {function, concept}
                            aggs.append(QueryAggregation(**agg))
                    plan_dict["aggregations"] = aggs

                # Convert intent string to enum
                plan_dict["intent"] = QueryIntent(plan_dict["intent"])

                # Create SemanticQueryPlan
                query_plan = SemanticQueryPlan(**plan_dict)

                # Validate that all concepts exist in knowledge graph
                # Extract concepts from filters and projections
                all_concepts = set()
                for f in query_plan.filters:
                    all_concepts.add(f.concept)
                if query_plan.projections:
                    all_concepts.update(query_plan.projections)
                if query_plan.aggregations:
                    for agg in query_plan.aggregations:
                        all_concepts.add(agg.concept)
                
                for concept in all_concepts:
                    if concept not in self.kg.concepts:
                        raise ValueError(
                            f"Unknown semantic concept: {concept}. "
                            f"Available concepts: {list(self.kg.concepts.keys())}"
                        )

                return query_plan

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                if attempt < max_retries:
                    # Retry with error feedback
                    user_prompt += f"\n\nPrevious attempt failed with error: {str(e)}\nPlease try again with valid JSON."
                    continue
                else:
                    raise ValueError(
                        f"Failed to parse query after {max_retries + 1} attempts. "
                        f"Last error: {str(e)}"
                    )

        raise ValueError("Unexpected error in query understanding")

    def explain_query_plan(self, query_plan: SemanticQueryPlan) -> str:
        """
        Generate a human-readable explanation of a query plan.

        Args:
            query_plan: The semantic query plan to explain

        Returns:
            Human-readable explanation
        """
        parts = []

        # Intent
        intent_desc = {
            QueryIntent.FIND_CONTRACTS: "Find contracts",
            QueryIntent.COUNT_CONTRACTS: "Count contracts",
            QueryIntent.AGGREGATE_VALUES: "Calculate statistics for contracts",
            QueryIntent.COMPARE_CUSTOMERS: "Compare customers",
            QueryIntent.GROUP_BY: "Group contracts",
        }
        parts.append(intent_desc.get(query_plan.intent, "Query"))

        # Filters
        if query_plan.filters:
            filter_parts = []
            for f in query_plan.filters:
                op_desc = {
                    QueryOperator.EQUALS: "equals",
                    QueryOperator.NOT_EQUALS: "does not equal",
                    QueryOperator.GREATER_THAN: "greater than",
                    QueryOperator.LESS_THAN: "less than",
                    QueryOperator.GREATER_THAN_OR_EQUAL: "at least",
                    QueryOperator.LESS_THAN_OR_EQUAL: "at most",
                    QueryOperator.CONTAINS: "contains",
                    QueryOperator.IN: "is one of",
                    QueryOperator.BETWEEN: "between",
                }
                concept_name = f.concept.replace("_", " ")
                op = op_desc.get(f.operator, str(f.operator))
                filter_parts.append(f"{concept_name} {op} {f.value}")

            parts.append("where " + " AND ".join(filter_parts))

        # Aggregations
        if query_plan.aggregations:
            parts.append(f"({', '.join(query_plan.aggregations)})")

        # Limit
        if query_plan.limit:
            parts.append(f"(limit {query_plan.limit})")

        return " ".join(parts)
