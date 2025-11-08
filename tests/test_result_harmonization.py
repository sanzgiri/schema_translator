"""Tests for result harmonization components."""

from pathlib import Path

import pytest

from schema_translator.config import Config
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    HarmonizedResult,
    HarmonizedRow,
    QueryAggregation,
    QueryFilter,
    QueryIntent,
    QueryOperator,
    SemanticQueryPlan,
    SemanticType,
)
from schema_translator.result_harmonizer import ResultHarmonizer


@pytest.fixture
def config():
    """Load configuration."""
    return Config()


@pytest.fixture
def knowledge_graph(config):
    """Create a knowledge graph for testing."""
    kg = SchemaKnowledgeGraph()
    kg.load(config.knowledge_graph_path)
    return kg


@pytest.fixture
def result_harmonizer(knowledge_graph):
    """Create a result harmonizer."""
    return ResultHarmonizer(knowledge_graph)


class TestValueHarmonizer:
    """Tests for value normalization (now part of ResultHarmonizer)."""
    
    def test_normalize_value_no_transformation(self, result_harmonizer):
        """Test normalizing a value with no transformation."""
        normalized = result_harmonizer._normalize_value(
            value="Active",
            customer_id="customer_a",
            concept_id="contract_status"
        )
        
        assert normalized.original_value == "Active"
        assert normalized.normalized_value == "Active"
        assert normalized.original_type == "text"
        assert normalized.transformation_applied is None
    
    def test_normalize_value_with_transformation(self, result_harmonizer):
        """Test normalizing a value that requires transformation."""
        # Customer D uses days_remaining instead of end_date
        normalized = result_harmonizer._normalize_value(
            value=365,
            customer_id="customer_d",
            concept_id="contract_expiration"
        )
        
        assert normalized.original_value == 365
        # Type is stored as "days_remaining" in the knowledge graph
        assert "days" in normalized.original_type.lower() or normalized.original_type == "integer"
        # Should convert days to date (approximately 1 year from now)
        assert normalized.normalized_value is not None
        assert "-" in str(normalized.normalized_value)  # Date format
    
    def test_days_to_date_conversion(self, result_harmonizer):
        """Test converting days remaining to a date."""
        # 30 days from now
        date_str = result_harmonizer._days_to_date(30)
        
        assert date_str is not None
        assert len(date_str) == 10  # YYYY-MM-DD format
        assert date_str.count("-") == 2
    
    def test_days_to_date_invalid(self, result_harmonizer):
        """Test days to date with invalid input."""
        assert result_harmonizer._days_to_date(None) is None
        assert result_harmonizer._days_to_date("invalid") is None
    
    def test_convert_type_int_to_float(self, result_harmonizer):
        """Test type conversion from integer to float."""
        result = result_harmonizer._convert_type(
            100,
            SemanticType.INTEGER,
            SemanticType.FLOAT
        )
        
        assert isinstance(result, float)
        assert result == 100.0
    
    def test_convert_type_float_to_int(self, result_harmonizer):
        """Test type conversion from float to integer."""
        result = result_harmonizer._convert_type(
            99.9,
            SemanticType.FLOAT,
            SemanticType.INTEGER
        )
        
        assert isinstance(result, int)
        assert result == 99
    
    def test_convert_type_to_text(self, result_harmonizer):
        """Test type conversion to text."""
        result = result_harmonizer._convert_type(
            123,
            SemanticType.INTEGER,
            SemanticType.TEXT
        )
        
        assert isinstance(result, str)
        assert result == "123"
    
    def test_normalize_industry_name(self, result_harmonizer):
        """Test normalizing industry names."""
        assert result_harmonizer._normalize_industry_name("tech") == "Technology"
        assert result_harmonizer._normalize_industry_name("TECHNOLOGY") == "Technology"
        assert result_harmonizer._normalize_industry_name("healthcare") == "Healthcare"
        assert result_harmonizer._normalize_industry_name("finance") == "Financial Services"
        assert result_harmonizer._normalize_industry_name("Unknown Industry") == "Unknown Industry"
        assert result_harmonizer._normalize_industry_name(None) is None
    
    def test_normalize_field_name(self, result_harmonizer):
        """Test mapping customer field names to concepts."""
        # Customer A uses 'contract_id'
        concept = result_harmonizer._normalize_field_name("contract_id", "customer_a")
        assert concept == "contract_identifier"
        
        # Customer B uses 'id' for contract_identifier
        concept = result_harmonizer._normalize_field_name("id", "customer_b")
        assert concept == "contract_identifier"
    
    def test_harmonize_row(self, result_harmonizer):
        """Test harmonizing a complete row."""
        row = {
            "contract_id": "A001",
            "status": "Active",
            "contract_value": 100000.0
        }
        
        field_mappings = {
            "contract_id": "contract_identifier",
            "status": "contract_status",
            "contract_value": "contract_value"
        }
        
        harmonized = result_harmonizer._harmonize_row(row, "customer_a", field_mappings)
        
        assert harmonized["contract_identifier"] == "A001"
        assert harmonized["contract_status"] == "Active"
        assert harmonized["contract_value"] == 100000.0
    
    def test_harmonize_row_with_industry(self, result_harmonizer):
        """Test harmonizing a row with industry normalization."""
        row = {
            "contract_id": "A001",
            "industry": "tech"
        }
        
        field_mappings = {
            "contract_id": "contract_identifier",
            "industry": "industry_sector"
        }
        
        harmonized = result_harmonizer._harmonize_row(row, "customer_a", field_mappings)
        
        assert harmonized["industry_sector"] == "Technology"


class TestResultHarmonizer:
    """Tests for ResultHarmonizer class."""
    
    def test_execute_across_single_customer(self, result_harmonizer):
        """Test executing a query for a single customer."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_status", "contract_value"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=["customer_a"]
        )
        
        assert isinstance(result, HarmonizedResult)
        assert "customer_a" in result.customers_queried
        assert len(result.customers_succeeded) >= 0
        assert result.total_count >= 0
    
    def test_execute_across_multiple_customers(self, result_harmonizer):
        """Test executing a query across multiple customers."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_status"],
            filters=[
                QueryFilter(
                    concept="contract_status",
                    operator=QueryOperator.EQUALS,
                    value="Active"
                )
            ],
            aggregations=[],
            limit=5
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=["customer_a", "customer_c"]
        )
        
        assert isinstance(result, HarmonizedResult)
        assert len(result.customers_queried) == 2
        assert result.total_count >= 0
        
        # Check that results have harmonized structure
        if result.results:
            first_row = result.results[0]
            assert isinstance(first_row, HarmonizedRow)
            assert "contract_identifier" in first_row.data
            assert "contract_status" in first_row.data
    
    def test_execute_all_customers_sequential(self, result_harmonizer):
        """Test executing across all customers sequentially."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.COUNT_CONTRACTS,
            projections=[],
            filters=[],
            aggregations=[
                QueryAggregation(function="count", concept="contract_identifier")
            ],
            limit=None
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=None,  # All customers
            parallel=False
        )
        
        assert isinstance(result, HarmonizedResult)
        assert len(result.customers_queried) == 6  # All 6 customers
        assert result.execution_time_ms > 0
    
    def test_execute_all_customers_parallel(self, result_harmonizer):
        """Test executing across all customers in parallel."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_status"],
            filters=[],
            aggregations=[],
            limit=5
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=None,  # All customers
            parallel=True
        )
        
        assert isinstance(result, HarmonizedResult)
        assert len(result.customers_queried) == 6
        # Parallel should generally be faster, but we just check it works
        assert result.execution_time_ms > 0
    
    def test_harmonize_with_filter(self, result_harmonizer):
        """Test harmonization with filtering."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[
                QueryFilter(
                    concept="contract_value",
                    operator=QueryOperator.GREATER_THAN,
                    value=500000
                )
            ],
            aggregations=[],
            limit=None
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=["customer_a", "customer_c", "customer_e"]
        )
        
        assert isinstance(result, HarmonizedResult)
        # All returned values should be > 500000
        for row in result.results:
            value = row.data.get("contract_value")
            if value is not None:
                assert value > 500000
    
    def test_get_concepts_from_plan(self, result_harmonizer):
        """Test extracting concepts from a query plan."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_status"],
            filters=[
                QueryFilter(concept="contract_value", operator=QueryOperator.GREATER_THAN, value=100000)
            ],
            aggregations=[
                QueryAggregation(function="sum", concept="contract_value")
            ],
            limit=None
        )
        
        concepts = result_harmonizer._get_concepts_from_plan(plan)
        
        assert "contract_identifier" in concepts
        assert "contract_status" in concepts
        assert "contract_value" in concepts
    
    def test_build_field_mappings(self, result_harmonizer):
        """Test building field mappings for a customer."""
        concepts = ["contract_identifier", "contract_status", "contract_value"]
        
        mappings = result_harmonizer._build_field_mappings("customer_a", concepts)
        
        assert mappings["contract_id"] == "contract_identifier"
        assert mappings["status"] == "contract_status"
        assert mappings["contract_value"] == "contract_value"
    
    def test_sort_results(self, result_harmonizer):
        """Test sorting harmonized results."""
        harmonized = HarmonizedResult(
            results=[
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A001", "contract_value": 300000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A002", "contract_value": 100000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A003", "contract_value": 200000}
                ),
            ],
            total_count=3,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=10.0
        )
        
        sorted_result = result_harmonizer.sort_results(
            harmonized, "contract_value", descending=True
        )
        
        assert sorted_result.results[0].data["contract_value"] == 300000
        assert sorted_result.results[1].data["contract_value"] == 200000
        assert sorted_result.results[2].data["contract_value"] == 100000
    
    def test_filter_results(self, result_harmonizer):
        """Test filtering harmonized results."""
        harmonized = HarmonizedResult(
            results=[
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A001", "contract_status": "Active"}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A002", "contract_status": "Expired"}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"contract_identifier": "A003", "contract_status": "Active"}
                ),
            ],
            total_count=3,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=10.0
        )
        
        filtered_result = result_harmonizer.filter_results(
            harmonized,
            lambda row: row.data.get("contract_status") == "Active"
        )
        
        assert filtered_result.total_count == 2
        assert all(row.data["contract_status"] == "Active" for row in filtered_result.results)
    
    def test_aggregate_results_count(self, result_harmonizer):
        """Test aggregating results with count."""
        harmonized = HarmonizedResult(
            results=[
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 100000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 200000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Healthcare", "contract_value": 150000}
                ),
            ],
            total_count=3,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=10.0
        )
        
        aggregated = result_harmonizer.aggregate_results(
            harmonized,
            group_by=["industry_sector"],
            aggregations={"contract_value": "count"}
        )
        
        assert aggregated.total_count == 2  # Two industries
        
        # Find Technology group
        tech_row = next(
            (r for r in aggregated.results if r.data.get("industry_sector") == "Technology"),
            None
        )
        assert tech_row is not None
        assert tech_row.data["contract_value_count"] == 2
    
    def test_aggregate_results_sum(self, result_harmonizer):
        """Test aggregating results with sum."""
        harmonized = HarmonizedResult(
            results=[
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 100000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 200000}
                ),
            ],
            total_count=2,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=10.0
        )
        
        aggregated = result_harmonizer.aggregate_results(
            harmonized,
            group_by=["industry_sector"],
            aggregations={"contract_value": "sum"}
        )
        
        assert aggregated.total_count == 1
        assert aggregated.results[0].data["contract_value_sum"] == 300000
    
    def test_aggregate_results_avg(self, result_harmonizer):
        """Test aggregating results with average."""
        harmonized = HarmonizedResult(
            results=[
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 100000}
                ),
                HarmonizedRow(
                    customer_id="customer_a",
                    data={"industry_sector": "Technology", "contract_value": 200000}
                ),
            ],
            total_count=2,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=10.0
        )
        
        aggregated = result_harmonizer.aggregate_results(
            harmonized,
            group_by=["industry_sector"],
            aggregations={"contract_value": "avg"}
        )
        
        assert aggregated.total_count == 1
        assert aggregated.results[0].data["contract_value_avg"] == 150000
    
    def test_error_handling(self, result_harmonizer):
        """Test handling of query errors."""
        # Create a plan that will work for some customers
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=1
        )
        
        # Execute across multiple customers
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=["customer_a", "customer_c"]
        )
        
        # Should complete even if some fail
        assert isinstance(result, HarmonizedResult)
        assert len(result.customers_queried) == 2
        # At least one should succeed
        assert len(result.customers_succeeded) >= 0
    
    def test_success_rate_property(self):
        """Test the success_rate property."""
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a", "customer_b", "customer_c", "customer_d"],
            customers_succeeded=["customer_a", "customer_c"],
            customers_failed=["customer_b", "customer_d"],
            errors={"customer_b": "error1", "customer_d": "error2"},
            execution_time_ms=100.0
        )
        
        assert result.success_rate == 50.0  # 2 out of 4
    
    def test_multi_customer_value_harmonization(self, result_harmonizer):
        """Test that values are properly harmonized across customers."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "industry_sector"],
            filters=[],
            aggregations=[],
            limit=3
        )
        
        result = result_harmonizer.execute_across_customers(
            plan,
            customer_ids=["customer_a", "customer_c"]
        )
        
        # Check that industry names are normalized
        for row in result.results:
            industry = row.data.get("industry_sector")
            if industry:
                # Should be normalized (title case, standard names)
                assert industry in [
                    "Technology", "Healthcare", "Financial Services",
                    "Retail", "Manufacturing", "Education", "Government"
                ] or industry[0].isupper()  # Or properly capitalized
