"""Data models for Schema Translator using Pydantic."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# Enums
class SemanticType(str, Enum):
    """Semantic types for values."""
    LIFETIME_TOTAL = "lifetime_total"
    ANNUAL_RECURRING_REVENUE = "annual_recurring_revenue"
    DATE = "date"
    DAYS_REMAINING = "days_remaining"
    TEXT = "text"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"


class ContractStatus(str, Enum):
    """Contract status values."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    PENDING = "pending"
    RENEWED = "renewed"


class QueryOperator(str, Enum):
    """Query filter operators."""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    BETWEEN = "between"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    WITHIN_NEXT_DAYS = "within_next_days"
    DATE_RANGE = "date_range"


class QueryIntent(str, Enum):
    """Query intent types."""
    FIND_CONTRACTS = "find_contracts"
    COUNT_CONTRACTS = "count_contracts"
    AGGREGATE_VALUES = "aggregate_values"
    COMPARE_CUSTOMERS = "compare_customers"
    GROUP_BY = "group_by"


# Schema Models
class SchemaColumn(BaseModel):
    """Represents a column in a database schema."""
    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="SQL data type (TEXT, INTEGER, REAL, DATE, etc.)")
    semantic_meaning: Optional[str] = Field(None, description="Semantic concept this column represents")
    semantic_type: Optional[SemanticType] = Field(None, description="Semantic type of values")
    transformations: List[str] = Field(default_factory=list, description="Required transformations")
    sample_values: List[Any] = Field(default_factory=list, description="Sample values from this column")
    is_primary_key: bool = Field(default=False, description="Whether this is a primary key")
    is_foreign_key: bool = Field(default=False, description="Whether this is a foreign key")
    foreign_key_table: Optional[str] = Field(None, description="Referenced table if foreign key")
    
    model_config = {"use_enum_values": True}


class SchemaTable(BaseModel):
    """Represents a table in a database schema."""
    name: str = Field(..., description="Table name")
    columns: List[SchemaColumn] = Field(..., description="List of columns in this table")
    relationships: Dict[str, str] = Field(
        default_factory=dict,
        description="Relationships to other tables (table_name -> join_column)"
    )
    
    def get_column(self, name: str) -> Optional[SchemaColumn]:
        """Get a column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


class CustomerSchema(BaseModel):
    """Represents the complete schema for a customer database."""
    customer_id: str = Field(..., description="Unique customer identifier")
    customer_name: str = Field(..., description="Customer display name")
    tables: List[SchemaTable] = Field(..., description="Tables in this schema")
    semantic_notes: Dict[str, str] = Field(
        default_factory=dict,
        description="Notes about semantic meanings specific to this customer"
    )
    last_analyzed: Optional[datetime] = Field(None, description="When schema was last analyzed")
    
    def get_table(self, name: str) -> Optional[SchemaTable]:
        """Get a table by name."""
        for table in self.tables:
            if table.name == name:
                return table
        return None


# Semantic Concept Models
class ConceptMapping(BaseModel):
    """Maps a concept to a specific customer's schema."""
    customer_id: str = Field(..., description="Customer identifier")
    table_name: str = Field(..., description="Table containing this concept")
    column_name: str = Field(..., description="Column representing this concept")
    data_type: str = Field(..., description="SQL data type")
    semantic_type: SemanticType = Field(..., description="Semantic interpretation")
    transformation: Optional[str] = Field(None, description="SQL transformation needed")
    join_requirements: List[str] = Field(
        default_factory=list,
        description="Additional tables needed for JOIN"
    )
    
    model_config = {"use_enum_values": True}


class SemanticConcept(BaseModel):
    """Represents a semantic concept that spans multiple customer schemas."""
    concept_id: str = Field(..., description="Unique concept identifier")
    concept_name: str = Field(..., description="Human-readable concept name")
    description: str = Field(..., description="Description of this concept")
    aliases: List[str] = Field(default_factory=list, description="Alternative names for this concept")
    customer_mappings: Dict[str, ConceptMapping] = Field(
        default_factory=dict,
        description="Mappings per customer (customer_id -> mapping)"
    )
    
    def get_mapping(self, customer_id: str) -> Optional[ConceptMapping]:
        """Get mapping for a specific customer."""
        return self.customer_mappings.get(customer_id)


# Query Models
class QueryFilter(BaseModel):
    """Represents a filter condition in a query."""
    concept: str = Field(..., description="Semantic concept to filter on")
    operator: QueryOperator = Field(..., description="Filter operator")
    value: Any = Field(..., description="Filter value(s)")
    semantic_note: Optional[str] = Field(None, description="Note about semantic interpretation")
    
    model_config = {"use_enum_values": True}


class QueryAggregation(BaseModel):
    """Represents an aggregation in a query."""
    function: str = Field(..., description="Aggregation function (SUM, COUNT, AVG, MIN, MAX)")
    concept: str = Field(..., description="Concept to aggregate")
    alias: Optional[str] = Field(None, description="Alias for result column")


class SemanticQueryPlan(BaseModel):
    """Schema-independent query representation."""
    intent: QueryIntent = Field(..., description="Query intent")
    filters: List[QueryFilter] = Field(default_factory=list, description="Filter conditions")
    projections: List[str] = Field(default_factory=list, description="Concepts to return")
    aggregations: Optional[List[QueryAggregation]] = Field(None, description="Aggregations to perform")
    group_by: Optional[List[str]] = Field(None, description="Concepts to group by")
    order_by: Optional[List[tuple[str, str]]] = Field(
        None,
        description="Ordering (concept, direction) pairs"
    )
    limit: Optional[int] = Field(None, description="Maximum number of results")
    
    model_config = {"use_enum_values": True}


# Result Models
class QueryResult(BaseModel):
    """Result from executing a query against a customer database."""
    customer_id: str = Field(..., description="Customer this result is from")
    data: List[Dict[str, Any]] = Field(..., description="Query result rows")
    sql_executed: str = Field(..., description="SQL that was executed")
    execution_time_ms: float = Field(..., description="Execution time in milliseconds")
    row_count: int = Field(..., description="Number of rows returned")
    error: Optional[str] = Field(None, description="Error message if query failed")
    
    @property
    def success(self) -> bool:
        """Whether query executed successfully."""
        return self.error is None


class NormalizedValue(BaseModel):
    """Represents a value with both original and normalized forms."""
    original_value: Any = Field(..., description="Original value from database")
    normalized_value: Any = Field(..., description="Normalized value")
    original_type: str = Field(..., description="Original semantic type")
    normalized_type: str = Field(..., description="Normalized semantic type")
    transformation_applied: Optional[str] = Field(None, description="Transformation that was applied")


class HarmonizedRow(BaseModel):
    """A single row with harmonized/normalized values."""
    customer_id: str = Field(..., description="Source customer")
    data: Dict[str, Any] = Field(..., description="Harmonized field values")
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata about normalization"
    )


class HarmonizedResult(BaseModel):
    """Harmonized results from multiple customers."""
    results: List[HarmonizedRow] = Field(..., description="Harmonized result rows")
    total_count: int = Field(..., description="Total number of results")
    customers_queried: List[str] = Field(..., description="List of customer IDs queried")
    customers_succeeded: List[str] = Field(..., description="Customers with successful queries")
    customers_failed: List[str] = Field(default_factory=list, description="Customers with failed queries")
    errors: Dict[str, str] = Field(
        default_factory=dict,
        description="Error messages per customer (customer_id -> error)"
    )
    execution_time_ms: float = Field(..., description="Total execution time")
    
    @property
    def success_rate(self) -> float:
        """Percentage of customers that returned results successfully."""
        if not self.customers_queried:
            return 0.0
        return len(self.customers_succeeded) / len(self.customers_queried) * 100


# Feedback and Learning Models
class QueryFeedback(BaseModel):
    """User feedback on a query result."""
    query_text: str = Field(..., description="Original query text")
    semantic_plan: SemanticQueryPlan = Field(..., description="Semantic query plan used")
    feedback_type: str = Field(..., description="Type of feedback (incorrect, missing, good)")
    feedback_text: Optional[str] = Field(None, description="User's feedback comment")
    correct_result: Optional[Any] = Field(None, description="What the correct result should be")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When feedback was given")


class SchemaChange(BaseModel):
    """Detected change in a customer schema."""
    customer_id: str = Field(..., description="Customer with schema change")
    change_type: str = Field(..., description="Type of change (added_column, removed_column, type_change)")
    table_name: str = Field(..., description="Affected table")
    column_name: Optional[str] = Field(None, description="Affected column")
    old_value: Optional[Any] = Field(None, description="Previous value")
    new_value: Optional[Any] = Field(None, description="New value")
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When change was detected")
    requires_remapping: bool = Field(default=False, description="Whether concept mappings need update")
