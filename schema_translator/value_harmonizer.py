"""Value harmonization for normalizing data across customer schemas."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import NormalizedValue, SemanticType


class ValueHarmonizer:
    """Harmonizes values across different customer schemas."""
    
    def __init__(self, knowledge_graph: SchemaKnowledgeGraph):
        """Initialize the value harmonizer.
        
        Args:
            knowledge_graph: Knowledge graph with concept mappings and transformations
        """
        self.knowledge_graph = knowledge_graph
    
    def normalize_value(
        self,
        value: Any,
        customer_id: str,
        concept_id: str,
        target_type: Optional[SemanticType] = None
    ) -> NormalizedValue:
        """Normalize a value from a customer schema to a common format.
        
        Args:
            value: The value to normalize
            customer_id: Customer ID for context
            concept_id: The semantic concept this value represents
            target_type: Optional target semantic type to convert to
            
        Returns:
            NormalizedValue with original and normalized forms
        """
        # Get concept mapping for this customer
        mapping = self.knowledge_graph.get_mapping(concept_id, customer_id)
        if not mapping:
            # No mapping found, return as-is
            return NormalizedValue(
                original_value=value,
                normalized_value=value,
                original_type="unknown",
                normalized_type="unknown",
                transformation_applied=None
            )
        
        # Handle semantic_type as either SemanticType enum or string
        if isinstance(mapping.semantic_type, SemanticType):
            original_type = mapping.semantic_type.value
            semantic_type_enum = mapping.semantic_type
        else:
            original_type = str(mapping.semantic_type)
            semantic_type_enum = SemanticType(mapping.semantic_type)
        
        transformation = mapping.transformation
        
        # Apply transformation if specified
        if transformation:
            normalized = self._apply_transformation(
                value, transformation, customer_id, concept_id
            )
            transformation_applied = transformation
        else:
            normalized = value
            transformation_applied = None
        
        # Convert type if target specified
        if target_type and target_type != semantic_type_enum:
            normalized = self._convert_type(normalized, semantic_type_enum, target_type)
            if transformation_applied:
                transformation_applied += f" + type_conversion_to_{target_type.value}"
            else:
                transformation_applied = f"type_conversion_to_{target_type.value}"
        
        return NormalizedValue(
            original_value=value,
            normalized_value=normalized,
            original_type=original_type,
            normalized_type=target_type.value if target_type else original_type,
            transformation_applied=transformation_applied
        )
    
    def _apply_transformation(
        self,
        value: Any,
        transformation: str,
        customer_id: str,
        concept_id: str
    ) -> Any:
        """Apply a transformation to a value.
        
        Args:
            value: Value to transform
            transformation: Transformation SQL or expression
            customer_id: Customer ID for context
            concept_id: Concept ID for context
            
        Returns:
            Transformed value
        """
        # Handle days_remaining -> end_date conversion
        if "CURRENT_DATE" in transformation or "julianday" in transformation:
            return self._days_to_date(value)
        
        # Handle annual_value -> lifetime_value conversion
        if "contract_length" in transformation or "*" in transformation:
            # Need to get contract_length for this specific row
            # For now, apply a default multiplier (this should be done at query time)
            # In real implementation, this would need row-level context
            return value  # Return as-is; transformation happens at SQL level
        
        # Other transformations
        return value
    
    def _days_to_date(self, days_remaining: Any) -> Optional[str]:
        """Convert days remaining to an end date.
        
        Args:
            days_remaining: Number of days remaining
            
        Returns:
            ISO format date string or None if invalid
        """
        if days_remaining is None:
            return None
        
        try:
            days = int(days_remaining)
            end_date = datetime.now() + timedelta(days=days)
            return end_date.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None
    
    def _convert_type(
        self,
        value: Any,
        from_type: SemanticType,
        to_type: SemanticType
    ) -> Any:
        """Convert a value from one semantic type to another.
        
        Args:
            value: Value to convert
            from_type: Current semantic type
            to_type: Target semantic type
            
        Returns:
            Converted value
        """
        if value is None:
            return None
        
        # Date conversions
        if to_type == SemanticType.DATE:
            if from_type == SemanticType.INTEGER:
                # Assume integer is days remaining
                return self._days_to_date(value)
            elif from_type == SemanticType.TEXT:
                # Parse text date
                try:
                    dt = datetime.fromisoformat(str(value))
                    return dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    return str(value)
        
        # Numeric conversions
        if to_type == SemanticType.FLOAT:
            if from_type in (SemanticType.INTEGER, SemanticType.TEXT):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return value
        
        if to_type == SemanticType.INTEGER:
            if from_type in (SemanticType.FLOAT, SemanticType.TEXT):
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return value
        
        # Text conversion (always works)
        if to_type == SemanticType.TEXT:
            return str(value)
        
        # No conversion available
        return value
    
    def normalize_field_name(
        self,
        customer_field_name: str,
        customer_id: str
    ) -> Optional[str]:
        """Map a customer-specific field name to its semantic concept.
        
        Args:
            customer_field_name: Field name in customer schema
            customer_id: Customer ID
            
        Returns:
            Semantic concept ID or None if not mapped
        """
        # Check all concepts for this customer
        for concept_id in self.knowledge_graph.concepts.keys():
            mapping = self.knowledge_graph.get_mapping(concept_id, customer_id)
            if mapping and mapping.column_name == customer_field_name:
                return concept_id
        
        return None
    
    def normalize_industry_name(self, industry: Optional[str]) -> Optional[str]:
        """Normalize industry names to common format.
        
        Args:
            industry: Industry name from customer data
            
        Returns:
            Normalized industry name
        """
        if not industry:
            return None
        
        # Convert to lowercase for comparison
        industry_lower = industry.lower().strip()
        
        # Map common variations
        industry_mapping = {
            "tech": "Technology",
            "technology": "Technology",
            "it": "Technology",
            "information technology": "Technology",
            "healthcare": "Healthcare",
            "health": "Healthcare",
            "medical": "Healthcare",
            "finance": "Financial Services",
            "financial": "Financial Services",
            "financial services": "Financial Services",
            "banking": "Financial Services",
            "retail": "Retail",
            "manufacturing": "Manufacturing",
            "mfg": "Manufacturing",
            "education": "Education",
            "edu": "Education",
            "government": "Government",
            "gov": "Government",
            "public sector": "Government",
        }
        
        return industry_mapping.get(industry_lower, industry.title())
    
    def harmonize_row(
        self,
        row: Dict[str, Any],
        customer_id: str,
        field_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """Harmonize a single row of data.
        
        Args:
            row: Raw row data from customer database
            customer_id: Customer ID
            field_mappings: Map of customer field names to concept IDs
            
        Returns:
            Harmonized row with normalized field names and values
        """
        harmonized = {}
        
        for customer_field, concept_id in field_mappings.items():
            if customer_field in row:
                value = row[customer_field]
                
                # Special handling for industry
                if concept_id == "industry_sector":
                    harmonized[concept_id] = self.normalize_industry_name(value)
                else:
                    # Normalize the value
                    normalized = self.normalize_value(value, customer_id, concept_id)
                    harmonized[concept_id] = normalized.normalized_value
            else:
                # Field not present in row
                harmonized[concept_id] = None
        
        return harmonized
