"""Result harmonization for combining and normalizing multi-customer query results."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from schema_translator.database_executor import DatabaseExecutor
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    HarmonizedResult,
    HarmonizedRow,
    QueryResult,
    SemanticQueryPlan,
)
from schema_translator.query_compiler import QueryCompiler
from schema_translator.value_harmonizer import ValueHarmonizer


class ResultHarmonizer:
    """Harmonizes query results across multiple customer databases."""
    
    def __init__(
        self,
        knowledge_graph: SchemaKnowledgeGraph,
        executor: Optional[DatabaseExecutor] = None
    ):
        """Initialize the result harmonizer.
        
        Args:
            knowledge_graph: Knowledge graph with concept mappings
            executor: Database executor (creates new one if not provided)
        """
        self.knowledge_graph = knowledge_graph
        self.executor = executor or DatabaseExecutor()
        self.compiler = QueryCompiler(knowledge_graph)
        self.value_harmonizer = ValueHarmonizer(knowledge_graph)
    
    def execute_across_customers(
        self,
        query_plan: SemanticQueryPlan,
        customer_ids: Optional[List[str]] = None,
        parallel: bool = True
    ) -> HarmonizedResult:
        """Execute a semantic query across multiple customers and harmonize results.
        
        Args:
            query_plan: Semantic query plan to execute
            customer_ids: List of customer IDs to query (all if None)
            parallel: Whether to execute queries in parallel
            
        Returns:
            HarmonizedResult with combined and normalized data
        """
        start_time = time.time()
        
        # Determine which customers to query
        if customer_ids is None:
            # Get all customer IDs from database directory
            db_dir = self.executor.config.database_dir
            db_files = list(db_dir.glob("customer_*.db"))
            customer_ids = [f.stem for f in db_files]
        
        # Execute queries for each customer
        if parallel and len(customer_ids) > 1:
            results = self._execute_parallel(query_plan, customer_ids)
        else:
            results = self._execute_sequential(query_plan, customer_ids)
        
        # Harmonize results
        harmonized = self._harmonize_results(query_plan, results)
        
        # Calculate execution time
        execution_time_ms = (time.time() - start_time) * 1000
        harmonized.execution_time_ms = execution_time_ms
        
        return harmonized
    
    def _execute_parallel(
        self,
        query_plan: SemanticQueryPlan,
        customer_ids: List[str]
    ) -> Dict[str, QueryResult]:
        """Execute queries in parallel across customers.
        
        Args:
            query_plan: Semantic query plan
            customer_ids: List of customer IDs
            
        Returns:
            Map of customer_id to QueryResult
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=min(len(customer_ids), 10)) as executor:
            # Submit all tasks
            future_to_customer = {
                executor.submit(self._execute_for_customer, query_plan, cid): cid
                for cid in customer_ids
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_customer):
                customer_id = future_to_customer[future]
                try:
                    results[customer_id] = future.result()
                except Exception as e:
                    # Create error result
                    results[customer_id] = QueryResult(
                        customer_id=customer_id,
                        data=[],
                        sql_executed="",
                        execution_time_ms=0,
                        row_count=0,
                        error=str(e)
                    )
        
        return results
    
    def _execute_sequential(
        self,
        query_plan: SemanticQueryPlan,
        customer_ids: List[str]
    ) -> Dict[str, QueryResult]:
        """Execute queries sequentially across customers.
        
        Args:
            query_plan: Semantic query plan
            customer_ids: List of customer IDs
            
        Returns:
            Map of customer_id to QueryResult
        """
        results = {}
        
        for customer_id in customer_ids:
            try:
                results[customer_id] = self._execute_for_customer(query_plan, customer_id)
            except Exception as e:
                # Create error result
                results[customer_id] = QueryResult(
                    customer_id=customer_id,
                    data=[],
                    sql_executed="",
                    execution_time_ms=0,
                    row_count=0,
                    error=str(e)
                )
        
        return results
    
    def _execute_for_customer(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str
    ) -> QueryResult:
        """Execute query for a single customer.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer ID
            
        Returns:
            QueryResult
        """
        # Compile query for this customer
        sql = self.compiler.compile_for_customer(query_plan, customer_id)
        
        # Execute query
        result = self.executor.execute_query(customer_id, sql)
        
        return result
    
    def _harmonize_results(
        self,
        query_plan: SemanticQueryPlan,
        results: Dict[str, QueryResult]
    ) -> HarmonizedResult:
        """Harmonize results from multiple customers.
        
        Args:
            query_plan: Original semantic query plan
            results: Map of customer_id to QueryResult
            
        Returns:
            HarmonizedResult with unified data
        """
        harmonized_rows = []
        customers_succeeded = []
        customers_failed = []
        errors = {}
        
        # Determine field mappings from query plan
        concepts = self._get_concepts_from_plan(query_plan)
        
        for customer_id, result in results.items():
            if result.error:
                customers_failed.append(customer_id)
                errors[customer_id] = result.error
                continue
            
            customers_succeeded.append(customer_id)
            
            # Get field mappings for this customer
            field_mappings = self._build_field_mappings(customer_id, concepts)
            
            # Harmonize each row
            for row in result.data:
                harmonized_data = self.value_harmonizer.harmonize_row(
                    row, customer_id, field_mappings
                )
                
                harmonized_rows.append(
                    HarmonizedRow(
                        customer_id=customer_id,
                        data=harmonized_data,
                        metadata={
                            "original_row": row,
                            "sql_executed": result.sql_executed
                        }
                    )
                )
        
        return HarmonizedResult(
            results=harmonized_rows,
            total_count=len(harmonized_rows),
            customers_queried=list(results.keys()),
            customers_succeeded=customers_succeeded,
            customers_failed=customers_failed,
            errors=errors,
            execution_time_ms=0  # Will be set by caller
        )
    
    def _get_concepts_from_plan(self, query_plan: SemanticQueryPlan) -> List[str]:
        """Extract concept IDs from query plan.
        
        Args:
            query_plan: Semantic query plan
            
        Returns:
            List of concept IDs
        """
        concepts = set()
        
        # Add projected concepts
        if query_plan.projections:
            concepts.update(query_plan.projections)
        
        # Add filtered concepts
        if query_plan.filters:
            for filter_obj in query_plan.filters:
                concepts.add(filter_obj.concept)
        
        # Add aggregated concepts
        if query_plan.aggregations:
            for agg in query_plan.aggregations:
                concepts.add(agg.concept)
        
        return list(concepts)
    
    def _build_field_mappings(
        self,
        customer_id: str,
        concepts: List[str]
    ) -> Dict[str, str]:
        """Build mapping from customer field names to concept IDs.
        
        Args:
            customer_id: Customer ID
            concepts: List of concept IDs to map
            
        Returns:
            Map of customer field name to concept ID
        """
        field_mappings = {}
        
        for concept_id in concepts:
            mapping = self.knowledge_graph.get_mapping(concept_id, customer_id)
            if mapping:
                field_mappings[mapping.column_name] = concept_id
        
        return field_mappings
    
    def aggregate_results(
        self,
        harmonized_result: HarmonizedResult,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, str]] = None
    ) -> HarmonizedResult:
        """Perform additional aggregation on harmonized results.
        
        Args:
            harmonized_result: Harmonized results to aggregate
            group_by: List of fields to group by
            aggregations: Map of field to aggregation function (count, sum, avg, etc.)
            
        Returns:
            New HarmonizedResult with aggregated data
        """
        if not group_by and not aggregations:
            return harmonized_result
        
        # Group rows
        groups: Dict[tuple, List[HarmonizedRow]] = {}
        
        for row in harmonized_result.results:
            if group_by:
                key = tuple(row.data.get(field) for field in group_by)
            else:
                key = ()  # Single group
            
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # Aggregate each group
        aggregated_rows = []
        
        for key, group_rows in groups.items():
            aggregated_data = {}
            
            # Add group_by fields
            if group_by:
                for i, field in enumerate(group_by):
                    aggregated_data[field] = key[i]
            
            # Apply aggregations
            if aggregations:
                for field, agg_func in aggregations.items():
                    values = [
                        row.data.get(field)
                        for row in group_rows
                        if row.data.get(field) is not None
                    ]
                    
                    if agg_func == "count":
                        aggregated_data[f"{field}_count"] = len(values)
                    elif agg_func == "sum":
                        aggregated_data[f"{field}_sum"] = sum(values) if values else None
                    elif agg_func == "avg":
                        aggregated_data[f"{field}_avg"] = (
                            sum(values) / len(values) if values else None
                        )
                    elif agg_func == "min":
                        aggregated_data[f"{field}_min"] = min(values) if values else None
                    elif agg_func == "max":
                        aggregated_data[f"{field}_max"] = max(values) if values else None
            
            # Create aggregated row (use first customer_id from group)
            aggregated_rows.append(
                HarmonizedRow(
                    customer_id="aggregated",
                    data=aggregated_data,
                    metadata={
                        "row_count": len(group_rows),
                        "customers": list(set(r.customer_id for r in group_rows))
                    }
                )
            )
        
        return HarmonizedResult(
            results=aggregated_rows,
            total_count=len(aggregated_rows),
            customers_queried=harmonized_result.customers_queried,
            customers_succeeded=harmonized_result.customers_succeeded,
            customers_failed=harmonized_result.customers_failed,
            errors=harmonized_result.errors,
            execution_time_ms=harmonized_result.execution_time_ms
        )
    
    def sort_results(
        self,
        harmonized_result: HarmonizedResult,
        sort_by: str,
        descending: bool = False
    ) -> HarmonizedResult:
        """Sort harmonized results by a field.
        
        Args:
            harmonized_result: Results to sort
            sort_by: Field name to sort by
            descending: Whether to sort descending
            
        Returns:
            New HarmonizedResult with sorted data
        """
        sorted_rows = sorted(
            harmonized_result.results,
            key=lambda r: r.data.get(sort_by, ""),
            reverse=descending
        )
        
        return HarmonizedResult(
            results=sorted_rows,
            total_count=harmonized_result.total_count,
            customers_queried=harmonized_result.customers_queried,
            customers_succeeded=harmonized_result.customers_succeeded,
            customers_failed=harmonized_result.customers_failed,
            errors=harmonized_result.errors,
            execution_time_ms=harmonized_result.execution_time_ms
        )
    
    def filter_results(
        self,
        harmonized_result: HarmonizedResult,
        filter_func
    ) -> HarmonizedResult:
        """Filter harmonized results using a custom function.
        
        Args:
            harmonized_result: Results to filter
            filter_func: Function that takes a HarmonizedRow and returns bool
            
        Returns:
            New HarmonizedResult with filtered data
        """
        filtered_rows = [
            row for row in harmonized_result.results
            if filter_func(row)
        ]
        
        return HarmonizedResult(
            results=filtered_rows,
            total_count=len(filtered_rows),
            customers_queried=harmonized_result.customers_queried,
            customers_succeeded=harmonized_result.customers_succeeded,
            customers_failed=harmonized_result.customers_failed,
            errors=harmonized_result.errors,
            execution_time_ms=harmonized_result.execution_time_ms
        )
