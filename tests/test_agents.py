"""
Tests for LLM agents (query understanding and schema analysis).
"""

import pytest
from schema_translator.agents import QueryUnderstandingAgent, SchemaAnalyzerAgent
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    QueryIntent,
    QueryOperator,
    SemanticType,
    CustomerSchema,
    SchemaTable,
    SchemaColumn,
)
from schema_translator.config import Config


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
def query_agent(config, knowledge_graph):
    """Create query understanding agent."""
    return QueryUnderstandingAgent(config.anthropic_api_key, knowledge_graph)


@pytest.fixture
def schema_agent(config, knowledge_graph):
    """Create schema analyzer agent."""
    return SchemaAnalyzerAgent(config.anthropic_api_key, knowledge_graph)


class TestQueryUnderstandingAgent:
    """Tests for natural language query understanding."""

    def test_simple_list_query(self, query_agent):
        """Test parsing a simple list query."""
        query = "Show me all active contracts"
        plan = query_agent.understand_query(query)

        assert plan.intent == QueryIntent.FIND_CONTRACTS
        assert len(plan.filters) >= 1
        assert any(f.concept == "contract_status" for f in plan.filters)

    def test_aggregation_query(self, query_agent):
        """Test parsing an aggregation query."""
        query = "How many contracts do we have?"
        plan = query_agent.understand_query(query)

        assert plan.intent == QueryIntent.COUNT_CONTRACTS
        assert plan.aggregations is not None
        assert len(plan.aggregations) > 0
        assert any(agg.function.lower() == "count" for agg in plan.aggregations)

    def test_filter_with_value(self, query_agent):
        """Test parsing a query with value filter."""
        query = "Find contracts worth more than 2 million dollars"
        plan = query_agent.understand_query(query)

        assert plan.intent in [QueryIntent.FIND_CONTRACTS]
        
        # Find the value filter
        value_filter = next(
            (f for f in plan.filters if f.concept == "contract_value"), None
        )
        assert value_filter is not None
        assert value_filter.operator in [
            QueryOperator.GREATER_THAN,
            QueryOperator.GREATER_THAN_OR_EQUAL,
        ]
        # Value should be around 2 million
        assert 1_000_000 <= float(value_filter.value) <= 3_000_000

    def test_date_range_query(self, query_agent):
        """Test parsing a query with date range."""
        query = "Show contracts expiring in the next 90 days"
        plan = query_agent.understand_query(query)

        assert len(plan.filters) >= 1
        
        # Should have a date filter
        date_filter = next(
            (f for f in plan.filters if f.concept == "contract_expiration"), None
        )
        assert date_filter is not None

    def test_multi_filter_query(self, query_agent):
        """Test parsing a query with multiple filters."""
        query = "List technology contracts expiring in 2026"
        plan = query_agent.understand_query(query)

        assert plan.intent == QueryIntent.FIND_CONTRACTS
        assert len(plan.filters) >= 2

    def test_explain_query_plan(self, query_agent):
        """Test generating human-readable explanations."""
        query = "Show me active contracts"
        plan = query_agent.understand_query(query)
        
        explanation = query_agent.explain_query_plan(plan)
        assert len(explanation) > 0
        assert "contract" in explanation.lower() or "active" in explanation.lower()


class TestSchemaAnalyzerAgent:
    """Tests for automated schema analysis."""

    def test_simple_schema_analysis(self, schema_agent):
        """Test analyzing a simple customer schema."""
        # Create a simple test schema
        schema = CustomerSchema(
            customer_id="test_customer",
            customer_name="Test Customer",
            tables=[
                SchemaTable(
                    name="contracts",
                    columns=[
                        SchemaColumn(
                            name="id",
                            data_type="INTEGER",
                            is_primary_key=True,
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="contract_number",
                            data_type="VARCHAR(50)",
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="value",
                            data_type="DECIMAL(15,2)",
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="expiration_date",
                            data_type="DATE",
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="status",
                            data_type="VARCHAR(20)",
                            # is_nullable not in model,
                        ),
                    ],
                )
            ],
        )

        mappings = schema_agent.analyze_schema("test_customer", schema)

        # Should find several mappings
        assert len(mappings) >= 3
        
        # Check that concepts are correctly identified
        concepts = {getattr(m, '_concept', 'unknown') for m in mappings}
        assert "contract_identifier" in concepts or "contract_value" in concepts

    def test_validate_mappings(self, schema_agent):
        """Test validating proposed mappings against schema."""
        from schema_translator.models import ConceptMapping

        schema = CustomerSchema(
            customer_id="test_customer",
            customer_name="Test Customer",
            tables=[
                SchemaTable(
                    name="contracts",
                    columns=[
                        SchemaColumn(
                            name="id",
                            data_type="INTEGER",
                            is_primary_key=True,
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="value",
                            data_type="DECIMAL(15,2)",
                            # is_nullable not in model,
                        ),
                    ],
                )
            ],
        )

        # Create valid and invalid mappings
        mappings = [
            ConceptMapping(
                customer_id="test_customer",
                table_name="contracts",
                column_name="value",
                data_type="DECIMAL(15,2)",
                semantic_type=SemanticType.FLOAT,
            ),
            ConceptMapping(
                customer_id="test_customer",
                table_name="nonexistent_table",
                column_name="value",
                data_type="DECIMAL(15,2)",
                semantic_type=SemanticType.FLOAT,
            ),
            ConceptMapping(
                customer_id="test_customer",
                table_name="contracts",
                column_name="nonexistent_column",
                data_type="DECIMAL(15,2)",
                semantic_type=SemanticType.FLOAT,
            ),
        ]

        valid, errors = schema_agent.validate_mappings(
            "test_customer", schema, mappings
        )

        # Should have 1 valid mapping
        assert len(valid) == 1
        assert valid[0].table_name == "contracts"
        assert valid[0].column_name == "value"

        # Should have 2 errors
        assert len(errors) == 2
        assert any("nonexistent_table" in e for e in errors)
        assert any("nonexistent_column" in e for e in errors)

    def test_explain_mappings(self, schema_agent):
        """Test generating human-readable mapping explanations."""
        from schema_translator.models import ConceptMapping

        mapping1 = ConceptMapping(
            customer_id="test",
            table_name="contracts",
            column_name="total_amount",
            data_type="DECIMAL(15,2)",
            semantic_type=SemanticType.FLOAT,
        )
        mapping1._concept = "contract_value"
        mapping1._confidence = 0.95
        
        mapping2 = ConceptMapping(
            customer_id="test",
            table_name="contracts",
            column_name="end_date",
            data_type="DATE",
            semantic_type=SemanticType.DATE,
            transformation="Direct mapping",
        )
        mapping2._concept = "contract_expiration"
        mapping2._confidence = 0.90
        
        mappings = [mapping1, mapping2]

        explanation = schema_agent.explain_mappings(mappings)

        assert len(explanation) > 0
        assert "contract" in explanation.lower()
        assert "contracts.total_amount" in explanation
        assert "95%" in explanation
        assert "Transformation" in explanation


class TestEndToEndAgent:
    """End-to-end tests with agents."""

    def test_query_to_execution(self, query_agent, config, knowledge_graph):
        """Test complete flow: NL query → semantic plan → SQL → execution."""
        from schema_translator.query_compiler import QueryCompiler
        from schema_translator.database_executor import DatabaseExecutor

        # Parse natural language query
        query = "Show me all active contracts"
        plan = query_agent.understand_query(query)

        # Compile to SQL
        compiler = QueryCompiler(knowledge_graph)
        customer_id = "customer_a"
        sql = compiler.compile_for_customer(plan, customer_id)

        # Execute query
        executor = DatabaseExecutor()
        results = executor.execute_query(customer_id, sql)

        # Should get results
        assert len(results.data) > 0
        assert results.execution_time_ms > 0

    def test_schema_analysis_to_knowledge_graph(
        self, schema_agent, config, knowledge_graph
    ):
        """Test analyzing a schema and adding mappings to knowledge graph."""
        # Create a test schema
        schema = CustomerSchema(
            customer_id="test_new_customer",
            customer_name="Test New Customer",
            tables=[
                SchemaTable(
                    name="agreements",
                    columns=[
                        SchemaColumn(
                            name="agreement_id",
                            data_type="VARCHAR(20)",
                            is_primary_key=True,
                            # is_nullable not in model,
                        ),
                        SchemaColumn(
                            name="worth",
                            data_type="DECIMAL(15,2)",
                            # is_nullable not in model,
                        ),
                    ],
                )
            ],
        )

        # Analyze schema
        mappings = schema_agent.analyze_schema("test_new_customer", schema)
        
        # Validate mappings
        valid_mappings, errors = schema_agent.validate_mappings(
            "test_new_customer", schema, mappings
        )

        # Should have valid mappings with no errors
        assert len(valid_mappings) > 0
        assert len(errors) == 0

        # Add to knowledge graph (in-memory only for test)
        for mapping in valid_mappings:
            concept = getattr(mapping, '_concept', None)
            if concept and concept in knowledge_graph.concepts:
                knowledge_graph.add_customer_mapping(
                    concept_id=concept,
                    customer_id=mapping.customer_id,
                    table_name=mapping.table_name,
                    column_name=mapping.column_name,
                    data_type=mapping.data_type,
                    semantic_type=mapping.semantic_type
                )

        # Verify we have mappings for known concepts
        concepts_with_mappings = [
            getattr(m, '_concept', None) for m in valid_mappings
        ]
        assert len(concepts_with_mappings) > 0
