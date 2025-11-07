"""Chat orchestrator for coordinating all schema translation components."""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from schema_translator.agents import QueryUnderstandingAgent, SchemaAnalyzerAgent
from schema_translator.config import Config
from schema_translator.database_executor import DatabaseExecutor
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    HarmonizedResult,
    QueryFeedback,
    SemanticQueryPlan,
)
from schema_translator.query_compiler import QueryCompiler
from schema_translator.result_harmonizer import ResultHarmonizer
from schema_translator.feedback_loop import FeedbackLoop
from schema_translator.schema_drift_detector import SchemaDriftDetector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QueryHistory:
    """Maintains history of executed queries."""
    
    def __init__(self):
        """Initialize query history."""
        self.queries: List[Dict[str, Any]] = []
    
    def add_query(
        self,
        query_text: str,
        semantic_plan: SemanticQueryPlan,
        result: HarmonizedResult,
        execution_time_ms: float,
        error: Optional[str] = None
    ):
        """Add a query to history.
        
        Args:
            query_text: Original natural language query
            semantic_plan: Parsed semantic query plan
            result: Query execution result
            execution_time_ms: Total execution time
            error: Error message if query failed
        """
        self.queries.append({
            "timestamp": datetime.utcnow(),
            "query_text": query_text,
            "semantic_plan": semantic_plan,
            "result": result,
            "execution_time_ms": execution_time_ms,
            "error": error,
            "success": error is None
        })
    
    def get_recent(self, n: int = 10) -> List[Dict[str, Any]]:
        """Get n most recent queries.
        
        Args:
            n: Number of queries to return
            
        Returns:
            List of recent query records
        """
        return self.queries[-n:]
    
    def get_failed_queries(self) -> List[Dict[str, Any]]:
        """Get all failed queries.
        
        Returns:
            List of failed query records
        """
        return [q for q in self.queries if not q["success"]]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query execution statistics.
        
        Returns:
            Statistics dictionary
        """
        if not self.queries:
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "success_rate": 0.0,
                "average_execution_time_ms": 0.0
            }
        
        successful = [q for q in self.queries if q["success"]]
        total_time = sum(q["execution_time_ms"] for q in self.queries)
        
        return {
            "total_queries": len(self.queries),
            "successful_queries": len(successful),
            "failed_queries": len(self.queries) - len(successful),
            "success_rate": len(successful) / len(self.queries) * 100,
            "average_execution_time_ms": total_time / len(self.queries)
        }


class ChatOrchestrator:
    """Orchestrates all components for natural language query processing."""
    
    def __init__(
        self,
        config: Optional[Config] = None,
        knowledge_graph: Optional[SchemaKnowledgeGraph] = None,
        use_llm: bool = True
    ):
        """Initialize the chat orchestrator.
        
        Args:
            config: Configuration object (creates new if None)
            knowledge_graph: Knowledge graph (loads from config if None)
            use_llm: Whether to use LLM agents (False for mock mode)
        """
        self.config = config or Config()
        self.use_llm = use_llm
        
        # Load knowledge graph
        if knowledge_graph is None:
            logger.info("Loading knowledge graph...")
            self.knowledge_graph = SchemaKnowledgeGraph()
            self.knowledge_graph.load(self.config.knowledge_graph_path)
        else:
            self.knowledge_graph = knowledge_graph
        
        # Initialize components
        logger.info("Initializing components...")
        self.executor = DatabaseExecutor()
        self.compiler = QueryCompiler(self.knowledge_graph)
        self.result_harmonizer = ResultHarmonizer(self.knowledge_graph, self.executor)
        
        # Initialize agents (if using LLM)
        if self.use_llm:
            logger.info("Initializing LLM agents...")
            self.query_agent = QueryUnderstandingAgent(
                self.config.anthropic_api_key,
                self.knowledge_graph
            )
            self.schema_agent = SchemaAnalyzerAgent(
                self.config.anthropic_api_key,
                self.knowledge_graph
            )
        else:
            logger.info("Running in mock mode (no LLM)")
            self.query_agent = None
            self.schema_agent = None
        
        # Initialize query history
        self.history = QueryHistory()
        
        # Initialize feedback loop
        self.feedback_loop = FeedbackLoop()
        
        # Initialize drift detector
        self.drift_detector = SchemaDriftDetector(
            self.executor,
            self.knowledge_graph
        )
        
        logger.info("ChatOrchestrator initialized successfully")
    
    def process_query(
        self,
        query_text: str,
        customer_ids: Optional[List[str]] = None,
        debug: bool = False
    ) -> Dict[str, Any]:
        """Process a natural language query end-to-end.
        
        Args:
            query_text: Natural language query
            customer_ids: Optional list of customer IDs to query (all if None)
            debug: Whether to include debug information
            
        Returns:
            Dictionary with results and metadata
        """
        start_time = time.time()
        
        logger.info(f"Processing query: '{query_text}'")
        
        try:
            # Step 1: Validate query
            if not self._validate_query(query_text):
                raise ValueError("Invalid query: Query text is empty or too short")
            
            # Step 2: Parse query to semantic plan
            logger.info("Parsing query to semantic plan...")
            semantic_plan = self._parse_query(query_text)
            
            if debug:
                logger.info(f"Semantic plan: {semantic_plan}")
            
            # Step 3: Execute query across customers
            logger.info(f"Executing query across {len(customer_ids) if customer_ids else 'all'} customers...")
            result = self.result_harmonizer.execute_across_customers(
                semantic_plan,
                customer_ids=customer_ids
            )
            
            # Step 4: Calculate total execution time
            total_time_ms = (time.time() - start_time) * 1000
            
            logger.info(
                f"Query completed: {result.total_count} rows, "
                f"{result.success_rate:.1f}% success rate, "
                f"{total_time_ms:.2f}ms"
            )
            
            # Step 5: Add to history
            self.history.add_query(
                query_text=query_text,
                semantic_plan=semantic_plan,
                result=result,
                execution_time_ms=total_time_ms,
                error=None
            )
            
            # Step 6: Build response
            response = {
                "success": True,
                "query_text": query_text,
                "semantic_plan": semantic_plan if debug else None,
                "result": result,
                "execution_time_ms": total_time_ms,
                "error": None
            }
            
            if debug:
                response["debug"] = self._build_debug_info(
                    semantic_plan,
                    result,
                    customer_ids
                )
            
            return response
            
        except Exception as e:
            error_msg = str(e)
            total_time_ms = (time.time() - start_time) * 1000
            
            logger.error(f"Query failed: {error_msg}", exc_info=True)
            
            # Add failed query to history
            self.history.add_query(
                query_text=query_text,
                semantic_plan=None,
                result=None,
                execution_time_ms=total_time_ms,
                error=error_msg
            )
            
            return {
                "success": False,
                "query_text": query_text,
                "semantic_plan": None,
                "result": None,
                "execution_time_ms": total_time_ms,
                "error": error_msg
            }
    
    def _validate_query(self, query_text: str) -> bool:
        """Validate a query before processing.
        
        Args:
            query_text: Query text to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not query_text or not query_text.strip():
            return False
        
        if len(query_text.strip()) < 3:
            return False
        
        return True
    
    def _parse_query(self, query_text: str) -> SemanticQueryPlan:
        """Parse natural language query to semantic plan.
        
        Args:
            query_text: Natural language query
            
        Returns:
            SemanticQueryPlan
        """
        if self.use_llm and self.query_agent:
            # Use LLM agent
            return self.query_agent.understand_query(query_text)
        else:
            # Mock mode: create a simple plan
            from schema_translator.models import QueryIntent
            return SemanticQueryPlan(
                intent=QueryIntent.FIND_CONTRACTS,
                projections=["contract_identifier", "contract_status", "contract_value"],
                filters=[],
                aggregations=[],
                limit=10
            )
    
    def _build_debug_info(
        self,
        semantic_plan: SemanticQueryPlan,
        result: HarmonizedResult,
        customer_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Build debug information for a query.
        
        Args:
            semantic_plan: Semantic query plan
            result: Execution result
            customer_ids: Customer IDs queried
            
        Returns:
            Debug information dictionary
        """
        debug_info = {
            "semantic_plan": {
                "intent": semantic_plan.intent.value if hasattr(semantic_plan.intent, 'value') else str(semantic_plan.intent),
                "projections": semantic_plan.projections,
                "filters": [
                    {
                        "concept": f.concept,
                        "operator": f.operator.value,
                        "value": f.value
                    }
                    for f in (semantic_plan.filters or [])
                ],
                "aggregations": [
                    {
                        "function": a.function,
                        "concept": a.concept
                    }
                    for a in (semantic_plan.aggregations or [])
                ],
                "limit": semantic_plan.limit
            },
            "customers": {
                "queried": result.customers_queried,
                "succeeded": result.customers_succeeded,
                "failed": result.customers_failed
            },
            "sql_queries": {}
        }
        
        # Add SQL for each customer
        target_customers = customer_ids if customer_ids else result.customers_queried
        for customer_id in target_customers[:3]:  # Limit to 3 for brevity
            try:
                sql = self.compiler.compile_for_customer(semantic_plan, customer_id)
                debug_info["sql_queries"][customer_id] = sql
            except Exception as e:
                debug_info["sql_queries"][customer_id] = f"Error: {e}"
        
        return debug_info
    
    def get_query_history(self, n: int = 10) -> List[Dict[str, Any]]:
        """Get recent query history.
        
        Args:
            n: Number of recent queries to return
            
        Returns:
            List of query records
        """
        return self.history.get_recent(n)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query execution statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = self.history.get_statistics()
        stats["knowledge_graph"] = self.knowledge_graph.get_stats()
        return stats
    
    def submit_feedback(
        self,
        query_text: str,
        semantic_plan: SemanticQueryPlan,
        feedback_type: str,
        feedback_text: Optional[str] = None
    ) -> QueryFeedback:
        """Submit feedback on a query result.
        
        Args:
            query_text: Original query text
            semantic_plan: Semantic query plan used
            feedback_type: Type of feedback (incorrect, missing, good)
            feedback_text: Optional feedback comment
            
        Returns:
            QueryFeedback object
        """
        # Submit to feedback loop
        feedback = self.feedback_loop.submit_feedback(
            query_text=query_text,
            semantic_plan=semantic_plan,
            feedback_type=feedback_type,
            feedback_text=feedback_text
        )
        
        logger.info(f"Feedback received: {feedback_type} for query '{query_text}'")
        
        return feedback
    
    def explain_query(self, query_text: str) -> Dict[str, Any]:
        """Explain how a query will be processed without executing it.
        
        Args:
            query_text: Natural language query
            
        Returns:
            Explanation dictionary
        """
        try:
            # Parse to semantic plan
            semantic_plan = self._parse_query(query_text)
            
            # Get human-readable explanation
            if self.use_llm and self.query_agent:
                explanation = self.query_agent.explain_query_plan(semantic_plan)
            else:
                explanation = f"Will find contracts with projections: {semantic_plan.projections}"
            
            # Get sample SQL for a few customers
            sample_sql = {}
            for customer_id in ["customer_a", "customer_b", "customer_c"]:
                try:
                    sql = self.compiler.compile_for_customer(semantic_plan, customer_id)
                    sample_sql[customer_id] = sql
                except Exception as e:
                    sample_sql[customer_id] = f"Error: {e}"
            
            return {
                "success": True,
                "query_text": query_text,
                "explanation": explanation,
                "semantic_plan": semantic_plan,
                "sample_sql": sample_sql
            }
            
        except Exception as e:
            return {
                "success": False,
                "query_text": query_text,
                "error": str(e)
            }
    
    def list_available_customers(self) -> List[str]:
        """Get list of available customer IDs.
        
        Returns:
            List of customer IDs
        """
        db_dir = self.config.database_dir
        db_files = list(db_dir.glob("customer_*.db"))
        return sorted([f.stem for f in db_files])
    
    def get_customer_info(self, customer_id: str) -> Dict[str, Any]:
        """Get information about a specific customer.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Customer information dictionary
        """
        try:
            # Get table info from database
            table_info = self.executor.get_table_info(customer_id)
            
            # Get concept mappings
            concepts = {}
            for concept_id in self.knowledge_graph.concepts.keys():
                mapping = self.knowledge_graph.get_mapping(concept_id, customer_id)
                if mapping:
                    concepts[concept_id] = {
                        "table": mapping.table_name,
                        "column": mapping.column_name,
                        "type": mapping.data_type,
                        "semantic_type": str(mapping.semantic_type),
                        "transformation": mapping.transformation
                    }
            
            # Get row count
            # Get the primary table name from table_info
            primary_table = None
            if table_info:
                primary_table = list(table_info.keys())[0] if table_info else None
            
            row_count = 0
            if primary_table:
                row_count = self.executor.count_rows(customer_id, primary_table)
            
            return {
                "customer_id": customer_id,
                "tables": table_info,
                "concepts": concepts,
                "total_rows": row_count,
                "available": True
            }
            
        except Exception as e:
            return {
                "customer_id": customer_id,
                "error": str(e),
                "available": False
            }
    
    def get_feedback_insights(self) -> Dict[str, Any]:
        """Get insights from user feedback.
        
        Returns:
            Feedback analysis and recommendations
        """
        return self.feedback_loop.get_improvement_recommendations()
    
    def check_schema_drift(
        self,
        customer_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Check for schema drift in customer databases.
        
        Args:
            customer_ids: Optional list of customers to check (all if None)
            
        Returns:
            Dictionary of drift information
        """
        if customer_ids:
            drifts = {}
            for customer_id in customer_ids:
                drift_list = self.drift_detector.detect_drift(customer_id)
                if drift_list:
                    drifts[customer_id] = [d.to_dict() for d in drift_list]
            return drifts
        else:
            # Check all customers
            all_drifts = self.drift_detector.check_all_customers()
            return {
                customer_id: [d.to_dict() for d in drifts]
                for customer_id, drifts in all_drifts.items()
            }
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health report.
        
        Returns:
            Comprehensive health report including feedback and drift
        """
        # Get feedback insights
        feedback_insights = self.get_feedback_insights()
        
        # Get drift summary
        drift_summary = self.drift_detector.get_drift_summary()
        
        # Get query statistics
        query_stats = self.get_statistics()
        
        # Determine overall health
        health_score = 100
        issues = []
        
        # Check query success rate
        if query_stats.get("success_rate", 0) < 80:
            health_score -= 20
            issues.append("Query success rate below 80%")
        
        # Check for critical drifts
        if drift_summary.get("critical_drifts"):
            health_score -= 30
            issues.append(f"{len(drift_summary['critical_drifts'])} critical schema drifts detected")
        
        # Check feedback health
        if feedback_insights.get("overall_health") == "needs_improvement":
            health_score -= 15
            issues.append("User feedback indicates issues")
        
        health_status = "excellent" if health_score >= 90 else \
                       "good" if health_score >= 70 else \
                       "fair" if health_score >= 50 else "poor"
        
        return {
            "health_status": health_status,
            "health_score": health_score,
            "issues": issues,
            "query_statistics": query_stats,
            "feedback_insights": feedback_insights,
            "drift_summary": drift_summary,
            "timestamp": datetime.utcnow().isoformat()
        }
