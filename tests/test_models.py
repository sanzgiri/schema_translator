"""Tests for data models."""

import json
from datetime import datetime

import pytest

from schema_translator.models import (
    ConceptMapping,
    ContractStatus,
    CustomerSchema,
    HarmonizedResult,
    HarmonizedRow,
    QueryAggregation,
    QueryFilter,
    QueryIntent,
    QueryOperator,
    QueryResult,
    SchemaColumn,
    SchemaTable,
    SemanticConcept,
    SemanticQueryPlan,
    SemanticType,
)


class TestSchemaModels:
    """Tests for schema-related models."""
    
    def test_schema_column_creation(self):
        """Test creating a SchemaColumn."""
        column = SchemaColumn(
            name="contract_id",
            data_type="INTEGER",
            semantic_meaning="contract_identifier",
            semantic_type=SemanticType.INTEGER,
            is_primary_key=True,
            sample_values=[1, 2, 3]
        )
        
        assert column.name == "contract_id"
        assert column.data_type == "INTEGER"
        assert column.is_primary_key is True
        assert len(column.sample_values) == 3
    
    def test_schema_column_json_serialization(self):
        """Test SchemaColumn JSON serialization."""
        column = SchemaColumn(
            name="contract_value",
            data_type="INTEGER",
            semantic_meaning="contract_value",
            semantic_type=SemanticType.LIFETIME_TOTAL
        )
        
        json_str = column.model_dump_json()
        assert "contract_value" in json_str
        
        # Deserialize
        column_dict = json.loads(json_str)
        column2 = SchemaColumn(**column_dict)
        assert column2.name == column.name
    
    def test_schema_table_creation(self):
        """Test creating a SchemaTable."""
        columns = [
            SchemaColumn(name="id", data_type="INTEGER", is_primary_key=True),
            SchemaColumn(name="name", data_type="TEXT")
        ]
        
        table = SchemaTable(
            name="contracts",
            columns=columns,
            relationships={"status_history": "contract_id"}
        )
        
        assert table.name == "contracts"
        assert len(table.columns) == 2
        assert table.get_column("id").is_primary_key is True
        assert table.get_column("nonexistent") is None
    
    def test_customer_schema_creation(self):
        """Test creating a CustomerSchema."""
        table = SchemaTable(
            name="contracts",
            columns=[
                SchemaColumn(name="id", data_type="INTEGER", is_primary_key=True)
            ]
        )
        
        schema = CustomerSchema(
            customer_id="customer_a",
            tables=[table],
            semantic_notes={"contract_value": "lifetime total"}
        )
        
        assert schema.customer_id == "customer_a"
        assert len(schema.tables) == 1
        assert schema.get_table("contracts") is not None
        assert schema.get_table("nonexistent") is None


class TestSemanticModels:
    """Tests for semantic concept models."""
    
    def test_concept_mapping_creation(self):
        """Test creating a ConceptMapping."""
        mapping = ConceptMapping(
            customer_id="customer_a",
            table_name="contracts",
            column_name="contract_value",
            data_type="INTEGER",
            semantic_type=SemanticType.LIFETIME_TOTAL
        )
        
        assert mapping.customer_id == "customer_a"
        assert mapping.semantic_type == SemanticType.LIFETIME_TOTAL
    
    def test_semantic_concept_creation(self):
        """Test creating a SemanticConcept."""
        mapping_a = ConceptMapping(
            customer_id="customer_a",
            table_name="contracts",
            column_name="contract_value",
            data_type="INTEGER",
            semantic_type=SemanticType.LIFETIME_TOTAL
        )
        
        concept = SemanticConcept(
            concept_id="contract_value",
            concept_name="Contract Value",
            description="Monetary value of the contract",
            aliases=["value", "amount"],
            customer_mappings={"customer_a": mapping_a}
        )
        
        assert concept.concept_id == "contract_value"
        assert len(concept.aliases) == 2
        assert concept.get_mapping("customer_a") is not None
        assert concept.get_mapping("customer_b") is None


class TestQueryModels:
    """Tests for query-related models."""
    
    def test_query_filter_creation(self):
        """Test creating a QueryFilter."""
        filter = QueryFilter(
            concept="contract_expiration",
            operator=QueryOperator.WITHIN_NEXT_DAYS,
            value=30,
            semantic_note="expiration may be date or days_remaining"
        )
        
        assert filter.concept == "contract_expiration"
        assert filter.operator == QueryOperator.WITHIN_NEXT_DAYS
        assert filter.value == 30
    
    def test_query_aggregation_creation(self):
        """Test creating a QueryAggregation."""
        agg = QueryAggregation(
            function="SUM",
            concept="contract_value",
            alias="total_value"
        )
        
        assert agg.function == "SUM"
        assert agg.concept == "contract_value"
    
    def test_semantic_query_plan_creation(self):
        """Test creating a SemanticQueryPlan."""
        filters = [
            QueryFilter(
                concept="contract_expiration",
                operator=QueryOperator.WITHIN_NEXT_DAYS,
                value=30
            )
        ]
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            filters=filters,
            projections=["contract_id", "contract_name", "contract_value"],
            limit=10
        )
        
        assert plan.intent == QueryIntent.FIND_CONTRACTS
        assert len(plan.filters) == 1
        assert len(plan.projections) == 3
        assert plan.limit == 10
    
    def test_semantic_query_plan_json_serialization(self):
        """Test SemanticQueryPlan JSON serialization."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.COUNT_CONTRACTS,
            filters=[],
            projections=[]
        )
        
        json_str = plan.model_dump_json()
        assert "count_contracts" in json_str
        
        # Deserialize
        plan_dict = json.loads(json_str)
        plan2 = SemanticQueryPlan(**plan_dict)
        assert plan2.intent == plan.intent


class TestResultModels:
    """Tests for result models."""
    
    def test_query_result_creation(self):
        """Test creating a QueryResult."""
        result = QueryResult(
            customer_id="customer_a",
            data=[{"id": 1, "name": "Contract 1"}],
            sql_executed="SELECT * FROM contracts",
            execution_time_ms=15.5,
            row_count=1
        )
        
        assert result.customer_id == "customer_a"
        assert result.success is True
        assert result.row_count == 1
    
    def test_query_result_with_error(self):
        """Test QueryResult with error."""
        result = QueryResult(
            customer_id="customer_b",
            data=[],
            sql_executed="SELECT * FROM invalid_table",
            execution_time_ms=5.0,
            row_count=0,
            error="Table does not exist"
        )
        
        assert result.success is False
        assert result.error is not None
    
    def test_harmonized_result_creation(self):
        """Test creating a HarmonizedResult."""
        rows = [
            HarmonizedRow(
                customer_id="customer_a",
                data={"contract_id": 1, "value": 1000000}
            ),
            HarmonizedRow(
                customer_id="customer_b",
                data={"contract_id": 2, "value": 2000000}
            )
        ]
        
        result = HarmonizedResult(
            results=rows,
            total_count=2,
            customers_queried=["customer_a", "customer_b"],
            customers_succeeded=["customer_a", "customer_b"],
            execution_time_ms=50.0
        )
        
        assert result.total_count == 2
        assert result.success_rate == 100.0
        assert len(result.customers_succeeded) == 2
    
    def test_harmonized_result_partial_success(self):
        """Test HarmonizedResult with partial success."""
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a", "customer_b", "customer_c"],
            customers_succeeded=["customer_a"],
            customers_failed=["customer_b", "customer_c"],
            errors={
                "customer_b": "Connection failed",
                "customer_c": "Invalid SQL"
            },
            execution_time_ms=30.0
        )
        
        assert result.success_rate == pytest.approx(33.33, rel=0.01)
        assert len(result.errors) == 2


class TestEnums:
    """Tests for enum types."""
    
    def test_semantic_type_enum(self):
        """Test SemanticType enum."""
        assert SemanticType.LIFETIME_TOTAL == "lifetime_total"
        assert SemanticType.ANNUAL_RECURRING_REVENUE == "annual_recurring_revenue"
    
    def test_query_operator_enum(self):
        """Test QueryOperator enum."""
        assert QueryOperator.EQUALS == "equals"
        assert QueryOperator.WITHIN_NEXT_DAYS == "within_next_days"
    
    def test_query_intent_enum(self):
        """Test QueryIntent enum."""
        assert QueryIntent.FIND_CONTRACTS == "find_contracts"
        assert QueryIntent.COUNT_CONTRACTS == "count_contracts"
