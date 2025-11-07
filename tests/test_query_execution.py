"""Tests for query compiler and database executor."""

import pytest

from schema_translator.database_executor import DatabaseExecutor
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    QueryFilter,
    QueryIntent,
    QueryOperator,
    SemanticQueryPlan,
)
from schema_translator.query_compiler import QueryCompiler


@pytest.fixture
def kg():
    """Load the knowledge graph."""
    kg = SchemaKnowledgeGraph()
    kg.load()
    return kg


@pytest.fixture
def compiler(kg):
    """Create a query compiler."""
    return QueryCompiler(kg)


@pytest.fixture
def executor():
    """Create a database executor."""
    return DatabaseExecutor()


class TestQueryCompiler:
    """Tests for the query compiler."""
    
    def test_simple_projection_customer_a(self, compiler):
        """Test simple projection for Customer A."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        assert "SELECT" in sql
        assert "contract_identifier" in sql
        assert "contract_value" in sql
        assert "FROM contracts" in sql
    
    def test_filter_with_value_threshold(self, compiler):
        """Test filtering by contract value."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[
                QueryFilter(
                    concept="contract_value",
                    operator=QueryOperator.GREATER_THAN,
                    value=1000000
                )
            ]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        assert "WHERE" in sql
        assert "> 1000000" in sql
    
    def test_date_filter_customer_a(self, compiler):
        """Test date filtering for Customer A (uses DATE type)."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_expiration"],
            filters=[
                QueryFilter(
                    concept="contract_expiration",
                    operator=QueryOperator.WITHIN_NEXT_DAYS,
                    value=30
                )
            ]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        assert "WHERE" in sql
        assert "BETWEEN" in sql
        assert "CURRENT_DATE" in sql
        assert "30 days" in sql
    
    def test_days_remaining_filter_customer_d(self, compiler):
        """Test filtering Customer D (uses days_remaining)."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_expiration"],
            filters=[
                QueryFilter(
                    concept="contract_expiration",
                    operator=QueryOperator.WITHIN_NEXT_DAYS,
                    value=30
                )
            ]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_d")
        
        assert "WHERE" in sql
        assert "days_remaining" in sql
        assert "BETWEEN 0 AND 30" in sql
    
    def test_annual_value_transformation_customer_f(self, compiler):
        """Test that Customer F's annual value gets transformed."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_f")
        
        # Should include transformation to lifetime
        assert "contract_value * term_years" in sql
    
    def test_multi_table_customer_b(self, compiler):
        """Test multi-table query for Customer B."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_expiration"],
            filters=[]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_b")
        
        # Should have JOIN
        assert "JOIN" in sql
        assert "contract_headers" in sql or "renewal_schedule" in sql
    
    def test_limit_clause(self, compiler):
        """Test LIMIT clause generation."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            limit=10
        )
        
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        assert "LIMIT 10" in sql
    
    def test_industry_filter(self, compiler):
        """Test filtering by industry."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "industry_sector"],
            filters=[
                QueryFilter(
                    concept="industry_sector",
                    operator=QueryOperator.EQUALS,
                    value="Technology"
                )
            ]
        )
        
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        assert "WHERE" in sql
        assert "industry" in sql
        assert "Technology" in sql


class TestDatabaseExecutor:
    """Tests for the database executor."""
    
    def test_connection(self, executor):
        """Test database connection."""
        assert executor.test_connection("customer_a")
        assert executor.test_connection("customer_b")
    
    def test_simple_query(self, executor):
        """Test executing a simple query."""
        sql = "SELECT contract_id, contract_name FROM contracts LIMIT 5"
        result = executor.execute_query("customer_a", sql)
        
        assert result.success
        assert result.row_count == 5
        assert len(result.data) == 5
        assert "contract_id" in result.data[0]
    
    def test_count_query(self, executor):
        """Test count query."""
        sql = "SELECT COUNT(*) as count FROM contracts"
        result = executor.execute_query("customer_a", sql)
        
        assert result.success
        assert result.row_count == 1
        assert result.data[0]["count"] == 50
    
    def test_invalid_sql(self, executor):
        """Test handling of invalid SQL."""
        sql = "SELECT * FROM nonexistent_table"
        result = executor.execute_query("customer_a", sql)
        
        assert not result.success
        assert result.error is not None
        assert "nonexistent_table" in result.error.lower() or "no such table" in result.error.lower()
    
    def test_get_table_info(self, executor):
        """Test getting table information."""
        info = executor.get_table_info("customer_a")
        
        assert "contracts" in info
        assert len(info["contracts"]) > 0
        
        # Check for expected columns
        column_names = [col["name"] for col in info["contracts"]]
        assert "contract_id" in column_names
        assert "contract_value" in column_names
    
    def test_count_rows(self, executor):
        """Test counting rows in a table."""
        count = executor.count_rows("customer_a", "contracts")
        assert count == 50
    
    def test_customer_b_multi_table(self, executor):
        """Test querying Customer B's multi-table schema."""
        sql = """
            SELECT h.id, h.contract_name, r.renewal_date
            FROM contract_headers AS h
            JOIN renewal_schedule AS r ON h.id = r.contract_id
            LIMIT 5
        """
        result = executor.execute_query("customer_b", sql)
        
        assert result.success
        assert result.row_count == 5
        assert "renewal_date" in result.data[0]
    
    def test_execution_time_recorded(self, executor):
        """Test that execution time is recorded."""
        sql = "SELECT * FROM contracts LIMIT 10"
        result = executor.execute_query("customer_a", sql)
        
        assert result.execution_time_ms >= 0
        assert result.execution_time_ms < 10000  # Should be fast
    
    def test_context_manager(self):
        """Test using executor as context manager."""
        with DatabaseExecutor() as executor:
            result = executor.execute_query("customer_a", "SELECT COUNT(*) as count FROM contracts")
            assert result.success


class TestIntegration:
    """Integration tests combining compiler and executor."""
    
    def test_end_to_end_simple_query(self, compiler, executor):
        """Test compiling and executing a simple query."""
        # Create query plan
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value", "customer_name"],
            filters=[],
            limit=10
        )
        
        # Compile for Customer A
        sql = compiler.compile_for_customer(plan, "customer_a")
        
        # Execute
        result = executor.execute_query("customer_a", sql)
        
        assert result.success
        assert result.row_count == 10
        assert "contract_identifier" in result.data[0]
        assert "contract_value" in result.data[0]
    
    def test_end_to_end_with_filter(self, compiler, executor):
        """Test compiling and executing with filters."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[
                QueryFilter(
                    concept="contract_value",
                    operator=QueryOperator.GREATER_THAN,
                    value=2000000
                )
            ],
            limit=20
        )
        
        # Test on multiple customers
        for customer_id in ["customer_a", "customer_c", "customer_d"]:
            sql = compiler.compile_for_customer(plan, customer_id)
            result = executor.execute_query(customer_id, sql)
            
            assert result.success, f"Failed for {customer_id}: {result.error}"
            
            # Verify all results have value > 2M
            for row in result.data:
                assert row["contract_value"] > 2000000
    
    def test_end_to_end_date_filter(self, compiler, executor):
        """Test date filtering across customers."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_expiration"],
            filters=[
                QueryFilter(
                    concept="contract_expiration",
                    operator=QueryOperator.WITHIN_NEXT_DAYS,
                    value=365
                )
            ]
        )
        
        # Test Customer A (DATE type)
        sql_a = compiler.compile_for_customer(plan, "customer_a")
        result_a = executor.execute_query("customer_a", sql_a)
        assert result_a.success
        
        # Test Customer D (days_remaining type)
        sql_d = compiler.compile_for_customer(plan, "customer_d")
        result_d = executor.execute_query("customer_d", sql_d)
        assert result_d.success
    
    def test_end_to_end_customer_f_annual_conversion(self, compiler, executor):
        """Test that Customer F's annual values are converted properly."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value"],
            filters=[],
            limit=5
        )
        
        # Compile for Customer F (should include transformation)
        sql = compiler.compile_for_customer(plan, "customer_f")
        
        # Execute
        result = executor.execute_query("customer_f", sql)
        
        assert result.success
        assert result.row_count == 5
        
        # Values should be lifetime (annual * term_years)
        # Check that values are reasonable (should be larger due to multiplication)
        for row in result.data:
            assert row["contract_value"] > 0
    
    def test_all_customers_same_query(self, compiler, executor):
        """Test executing the same semantic query across all customers."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier", "contract_value", "customer_name"],
            filters=[],
            limit=5
        )
        
        customers = ["customer_a", "customer_b", "customer_c", 
                    "customer_d", "customer_e", "customer_f"]
        
        for customer_id in customers:
            sql = compiler.compile_for_customer(plan, customer_id)
            result = executor.execute_query(customer_id, sql)
            
            assert result.success, f"Failed for {customer_id}: {result.error}"
            assert result.row_count == 5, f"Wrong row count for {customer_id}"
            assert len(result.data) == 5
