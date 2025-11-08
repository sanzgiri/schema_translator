"""Result harmonization for combining and normalizing multi-customer query results."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from schema_translator.database_executor import DatabaseExecutor
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    HarmonizedResult,
    HarmonizedRow,
    NormalizedValue,
    QueryResult,
    SemanticQueryPlan,
    SemanticType,
)
from schema_translator.query_compiler import QueryCompiler


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
                harmonized_data = self._harmonize_row(
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
        else:
            # If no projections specified (SELECT *), include all available concepts
            # This ensures all fields are harmonized when user asks for "all" data
            all_concepts = self.knowledge_graph.get_all_concepts()
            concepts.update([c.concept_id for c in all_concepts])
        
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
    
    # Value normalization methods (formerly ValueHarmonizer)
    
    def _normalize_value(
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
    
    def _normalize_field_name(
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
    
    def _normalize_industry_name(self, industry: Optional[str]) -> Optional[str]:
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
    
    def _harmonize_row(
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
                    harmonized[concept_id] = self._normalize_industry_name(value)
                else:
                    # Normalize the value
                    normalized = self._normalize_value(value, customer_id, concept_id)
                    harmonized[concept_id] = normalized.normalized_value
            else:
                # Field not present in row
                harmonized[concept_id] = None
        
        return harmonized
