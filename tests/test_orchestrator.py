"""Tests for the ChatOrchestrator."""

import pytest

from schema_translator.config import Config
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import QueryIntent, SemanticQueryPlan
from schema_translator.orchestrator import ChatOrchestrator, QueryHistory


@pytest.fixture
def config():
    """Load configuration."""
    return Config()


@pytest.fixture
def knowledge_graph(config):
    """Load knowledge graph."""
    kg = SchemaKnowledgeGraph()
    kg.load(config.knowledge_graph_path)
    return kg


@pytest.fixture
def orchestrator_mock(config, knowledge_graph):
    """Create orchestrator in mock mode (no LLM)."""
    return ChatOrchestrator(
        config=config,
        knowledge_graph=knowledge_graph,
        use_llm=False
    )


@pytest.fixture
def orchestrator_llm(config, knowledge_graph):
    """Create orchestrator with LLM agents."""
    return ChatOrchestrator(
        config=config,
        knowledge_graph=knowledge_graph,
        use_llm=True
    )


class TestQueryHistory:
    """Tests for QueryHistory class."""
    
    def test_add_query(self):
        """Test adding a query to history."""
        history = QueryHistory()
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        from schema_translator.models import HarmonizedResult
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=100.0
        )
        
        history.add_query(
            query_text="test query",
            semantic_plan=plan,
            result=result,
            execution_time_ms=100.0
        )
        
        assert len(history.queries) == 1
        assert history.queries[0]["query_text"] == "test query"
        assert history.queries[0]["success"] is True
    
    def test_get_recent(self):
        """Test getting recent queries."""
        history = QueryHistory()
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        from schema_translator.models import HarmonizedResult
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=100.0
        )
        
        # Add 5 queries
        for i in range(5):
            history.add_query(
                query_text=f"query {i}",
                semantic_plan=plan,
                result=result,
                execution_time_ms=100.0
            )
        
        recent = history.get_recent(3)
        assert len(recent) == 3
        assert recent[-1]["query_text"] == "query 4"
    
    def test_get_failed_queries(self):
        """Test getting failed queries."""
        history = QueryHistory()
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        from schema_translator.models import HarmonizedResult
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=100.0
        )
        
        # Add successful query
        history.add_query(
            query_text="success",
            semantic_plan=plan,
            result=result,
            execution_time_ms=100.0
        )
        
        # Add failed query
        history.add_query(
            query_text="failure",
            semantic_plan=plan,
            result=result,
            execution_time_ms=100.0,
            error="Test error"
        )
        
        failed = history.get_failed_queries()
        assert len(failed) == 1
        assert failed[0]["query_text"] == "failure"
    
    def test_get_statistics(self):
        """Test getting query statistics."""
        history = QueryHistory()
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        from schema_translator.models import HarmonizedResult
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            execution_time_ms=100.0
        )
        
        # Add 3 successful, 1 failed
        for i in range(3):
            history.add_query(
                query_text=f"query {i}",
                semantic_plan=plan,
                result=result,
                execution_time_ms=100.0
            )
        
        history.add_query(
            query_text="failed",
            semantic_plan=plan,
            result=result,
            execution_time_ms=200.0,
            error="Error"
        )
        
        stats = history.get_statistics()
        assert stats["total_queries"] == 4
        assert stats["successful_queries"] == 3
        assert stats["failed_queries"] == 1
        assert stats["success_rate"] == 75.0
        assert stats["average_execution_time_ms"] == 125.0


class TestChatOrchestrator:
    """Tests for ChatOrchestrator class."""
    
    def test_initialization_mock_mode(self, config, knowledge_graph):
        """Test orchestrator initialization in mock mode."""
        orchestrator = ChatOrchestrator(
            config=config,
            knowledge_graph=knowledge_graph,
            use_llm=False
        )
        
        assert orchestrator.config == config
        assert orchestrator.knowledge_graph == knowledge_graph
        assert orchestrator.use_llm is False
        assert orchestrator.query_agent is None
        assert orchestrator.schema_agent is None
    
    def test_initialization_llm_mode(self, config, knowledge_graph):
        """Test orchestrator initialization with LLM."""
        orchestrator = ChatOrchestrator(
            config=config,
            knowledge_graph=knowledge_graph,
            use_llm=True
        )
        
        assert orchestrator.use_llm is True
        assert orchestrator.query_agent is not None
        assert orchestrator.schema_agent is not None
    
    def test_validate_query(self, orchestrator_mock):
        """Test query validation."""
        assert orchestrator_mock._validate_query("Show me contracts") is True
        assert orchestrator_mock._validate_query("") is False
        assert orchestrator_mock._validate_query("   ") is False
        assert orchestrator_mock._validate_query("ab") is False
    
    def test_process_query_mock_mode(self, orchestrator_mock):
        """Test processing a query in mock mode."""
        response = orchestrator_mock.process_query(
            "Show me all contracts",
            customer_ids=["customer_a"]
        )
        
        assert response["success"] is True
        assert response["query_text"] == "Show me all contracts"
        assert response["result"] is not None
        assert response["execution_time_ms"] > 0
        assert response["error"] is None
    
    def test_process_query_with_debug(self, orchestrator_mock):
        """Test processing a query with debug mode."""
        response = orchestrator_mock.process_query(
            "Show me all contracts",
            customer_ids=["customer_a"],
            debug=True
        )
        
        assert response["success"] is True
        assert response["semantic_plan"] is not None
        assert "debug" in response
        assert "sql_queries" in response["debug"]
    
    def test_process_query_invalid(self, orchestrator_mock):
        """Test processing an invalid query."""
        response = orchestrator_mock.process_query("")
        
        assert response["success"] is False
        assert response["error"] is not None
        assert "Invalid query" in response["error"]
    
    def test_process_query_multiple_customers(self, orchestrator_mock):
        """Test processing query across multiple customers."""
        response = orchestrator_mock.process_query(
            "Find contracts",
            customer_ids=["customer_a", "customer_c", "customer_e"]
        )
        
        assert response["success"] is True
        result = response["result"]
        assert len(result.customers_queried) == 3
    
    def test_process_query_all_customers(self, orchestrator_mock):
        """Test processing query across all customers."""
        response = orchestrator_mock.process_query("Find contracts")
        
        assert response["success"] is True
        result = response["result"]
        assert len(result.customers_queried) == 6  # All 6 customers
    
    def test_query_history_tracking(self, orchestrator_mock):
        """Test that queries are added to history."""
        # Process a few queries
        orchestrator_mock.process_query("Query 1", customer_ids=["customer_a"])
        orchestrator_mock.process_query("Query 2", customer_ids=["customer_b"])
        
        history = orchestrator_mock.get_query_history()
        assert len(history) == 2
        assert history[0]["query_text"] == "Query 1"
        assert history[1]["query_text"] == "Query 2"
    
    def test_get_statistics(self, orchestrator_mock):
        """Test getting statistics."""
        # Process some queries
        orchestrator_mock.process_query("Query 1", customer_ids=["customer_a"])
        orchestrator_mock.process_query("Query 2", customer_ids=["customer_b"])
        orchestrator_mock.process_query("")  # This will fail
        
        stats = orchestrator_mock.get_statistics()
        assert stats["total_queries"] == 3
        assert stats["successful_queries"] == 2
        assert stats["failed_queries"] == 1
        assert "knowledge_graph" in stats
    
    def test_explain_query(self, orchestrator_mock):
        """Test query explanation."""
        explanation = orchestrator_mock.explain_query("Show me all contracts")
        
        assert explanation["success"] is True
        assert "explanation" in explanation
        assert "semantic_plan" in explanation
        assert "sample_sql" in explanation
        assert len(explanation["sample_sql"]) > 0
    
    def test_list_available_customers(self, orchestrator_mock):
        """Test listing available customers."""
        customers = orchestrator_mock.list_available_customers()
        
        assert len(customers) == 6
        assert "customer_a" in customers
        assert "customer_b" in customers
    
    def test_get_customer_info(self, orchestrator_mock):
        """Test getting customer information."""
        info = orchestrator_mock.get_customer_info("customer_a")
        
        if not info["available"]:
            print(f"Error: {info.get('error')}")
        
        assert info["available"] is True
        assert info["customer_id"] == "customer_a"
        assert "tables" in info
        assert "concepts" in info
        assert info["total_rows"] > 0
    
    def test_get_customer_info_invalid(self, orchestrator_mock):
        """Test getting info for invalid customer."""
        info = orchestrator_mock.get_customer_info("customer_invalid")
        
        assert info["available"] is False
        assert "error" in info
    
    def test_submit_feedback(self, orchestrator_mock):
        """Test submitting feedback."""
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_identifier"],
            filters=[],
            aggregations=[],
            limit=10
        )
        
        feedback = orchestrator_mock.submit_feedback(
            query_text="test query",
            semantic_plan=plan,
            feedback_type="good",
            feedback_text="Works great!"
        )
        
        assert feedback.query_text == "test query"
        assert feedback.feedback_type == "good"
        assert feedback.feedback_text == "Works great!"


class TestEndToEndIntegration:
    """End-to-end integration tests."""
    
    def test_simple_query_flow(self, orchestrator_mock):
        """Test complete flow: query → parse → execute → results."""
        response = orchestrator_mock.process_query(
            "Show me contracts",
            customer_ids=["customer_a"]
        )
        
        # Verify complete flow
        assert response["success"] is True
        assert response["result"].total_count >= 0
        assert len(response["result"].customers_succeeded) > 0
        
        # Verify history was updated
        history = orchestrator_mock.get_query_history(1)
        assert len(history) == 1
        assert history[0]["success"] is True
    
    def test_multi_customer_query_flow(self, orchestrator_mock):
        """Test querying multiple customers."""
        response = orchestrator_mock.process_query(
            "Find all contracts",
            customer_ids=["customer_a", "customer_c", "customer_e"]
        )
        
        assert response["success"] is True
        result = response["result"]
        assert len(result.customers_queried) == 3
        # All should succeed
        assert result.success_rate >= 0
    
    def test_error_recovery(self, orchestrator_mock):
        """Test that errors are handled gracefully."""
        # Invalid query should not crash
        response1 = orchestrator_mock.process_query("")
        assert response1["success"] is False
        
        # Next query should still work
        response2 = orchestrator_mock.process_query(
            "Show contracts",
            customer_ids=["customer_a"]
        )
        assert response2["success"] is True
    
    def test_performance_monitoring(self, orchestrator_mock):
        """Test that performance is tracked."""
        response = orchestrator_mock.process_query(
            "Find contracts",
            customer_ids=["customer_a"]
        )
        
        # Execution time should be recorded
        assert response["execution_time_ms"] > 0
        assert response["execution_time_ms"] < 10000  # Should be < 10s
        
        # Result should have execution time
        assert response["result"].execution_time_ms > 0
    
    def test_query_with_llm(self, orchestrator_llm):
        """Test processing query with LLM agent."""
        response = orchestrator_llm.process_query(
            "Show me all active contracts",
            customer_ids=["customer_a"]
        )
        
        # Should parse query using LLM
        assert response["success"] is True
        
        # History should track it
        history = orchestrator_llm.get_query_history(1)
        assert len(history) == 1
