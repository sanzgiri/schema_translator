"""Query compiler to generate customer-specific SQL from semantic query plans."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import (
    QueryFilter,
    QueryOperator,
    SemanticQueryPlan,
    SemanticType,
)


class QueryCompiler:
    """Compiles semantic query plans into customer-specific SQL."""
    
    def __init__(self, knowledge_graph: SchemaKnowledgeGraph):
        """Initialize the query compiler.
        
        Args:
            knowledge_graph: Knowledge graph with schema mappings
        """
        self.kg = knowledge_graph
    
    def compile_for_customer(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str
    ) -> str:
        """Compile a semantic query plan to SQL for a specific customer.
        
        Args:
            query_plan: Semantic query plan to compile
            customer_id: Customer identifier
            
        Returns:
            SQL query string
            
        Raises:
            ValueError: If required mappings are missing
        """
        # Collect all tables needed for this query
        tables_needed = self._get_required_tables(query_plan, customer_id)
        
        # Determine primary table
        primary_table = self._determine_primary_table(query_plan, customer_id, tables_needed)
        
        # Generate SELECT clause
        select_clause = self._generate_select(query_plan, customer_id, tables_needed, primary_table)
        
        # Generate FROM clause with JOINs if needed
        from_clause = self._generate_from(query_plan, customer_id, tables_needed)
        
        # Generate WHERE clause
        where_clause = self._generate_where(query_plan, customer_id, primary_table)
        
        # Generate GROUP BY clause
        group_by_clause = self._generate_group_by(query_plan, customer_id, primary_table)
        
        # Generate ORDER BY clause
        order_by_clause = self._generate_order_by(query_plan, customer_id, primary_table)
        
        # Generate LIMIT clause
        limit_clause = self._generate_limit(query_plan)
        
        # Assemble the query
        sql_parts = [select_clause, from_clause]
        
        if where_clause:
            sql_parts.append(where_clause)
        
        if group_by_clause:
            sql_parts.append(group_by_clause)
        
        if order_by_clause:
            sql_parts.append(order_by_clause)
        
        if limit_clause:
            sql_parts.append(limit_clause)
        
        return "\n".join(sql_parts)
    
    def _get_required_tables(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str
    ) -> Set[str]:
        """Get all tables required for this query.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            
        Returns:
            Set of table names needed
        """
        tables = set()
        
        # Get tables from projections
        if query_plan.projections:
            for concept_id in query_plan.projections:
                mapping = self.kg.get_mapping(concept_id, customer_id)
                if mapping:
                    # Skip tables with transformations (they use subqueries, not JOINs)
                    if not mapping.transformation:
                        tables.add(mapping.table_name)
                    tables.update(mapping.join_requirements)
        else:
            # If no projections specified (select all), get tables from all concepts
            all_concepts = self.kg.get_all_concepts()
            for concept in all_concepts:
                mapping = self.kg.get_mapping(concept.concept_id, customer_id)
                if mapping:
                    # Skip tables with transformations (they use subqueries, not JOINs)
                    if not mapping.transformation:
                        tables.add(mapping.table_name)
                    tables.update(mapping.join_requirements)
        
        # Get tables from filters
        for filter in query_plan.filters:
            mapping = self.kg.get_mapping(filter.concept, customer_id)
            if mapping:
                # If mapping has a transformation (subquery), don't add its table to joins
                # The transformation will be used directly in WHERE clause
                if not mapping.transformation:
                    tables.add(mapping.table_name)
                    tables.update(mapping.join_requirements)
                else:
                    # For transformations, only add join_requirements (not the transformed table itself)
                    tables.update(mapping.join_requirements)
        
        # Get tables from aggregations
        if query_plan.aggregations:
            for agg in query_plan.aggregations:
                mapping = self.kg.get_mapping(agg.concept, customer_id)
                if mapping:
                    tables.add(mapping.table_name)
                    tables.update(mapping.join_requirements)
        
        # Get tables from group_by
        if query_plan.group_by:
            for concept_id in query_plan.group_by:
                mapping = self.kg.get_mapping(concept_id, customer_id)
                if mapping:
                    tables.add(mapping.table_name)
                    tables.update(mapping.join_requirements)
        
        return tables
    
    def _generate_select(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        tables_needed: Optional[Set[str]] = None,
        primary_table: Optional[str] = None
    ) -> str:
        """Generate SELECT clause.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            tables_needed: Set of tables required (for DISTINCT determination)
            primary_table: Primary table name for the query
            
        Returns:
            SELECT clause
        """
        select_items = []
        
        # Handle aggregations
        if query_plan.aggregations:
            for agg in query_plan.aggregations:
                mapping = self.kg.get_mapping(agg.concept, customer_id)
                if not mapping:
                    raise ValueError(f"No mapping for concept '{agg.concept}' in {customer_id}")
                
                column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                alias = agg.alias or f"{agg.function.lower()}_{agg.concept}"
                select_items.append(f"{agg.function}({column_expr}) AS {alias}")
            
            # Add group by columns to select
            if query_plan.group_by:
                for concept_id in query_plan.group_by:
                    mapping = self.kg.get_mapping(concept_id, customer_id)
                    if mapping:
                        column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                        select_items.append(f"{column_expr} AS {concept_id}")
        
        # Handle regular projections
        elif query_plan.projections:
            for concept_id in query_plan.projections:
                mapping = self.kg.get_mapping(concept_id, customer_id)
                if not mapping:
                    raise ValueError(f"No mapping for concept '{concept_id}' in {customer_id}")
                
                column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                select_items.append(f"{column_expr} AS {concept_id}")
        
        else:
            # Select all conceptual fields if no projections specified
            # For multi-table queries, explicitly select columns to avoid foreign key columns
            if tables_needed and len(tables_needed) > 1:
                # Get all concepts that map to this customer
                all_concepts = self.kg.get_all_concepts()
                for concept in all_concepts:
                    mapping = self.kg.get_mapping(concept.concept_id, customer_id)
                    if mapping and mapping.table_name in tables_needed:
                        # Skip if transformation - those are handled via subqueries
                        if not mapping.transformation:
                            column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                            select_items.append(f"{column_expr} AS {concept.concept_id}")
                        else:
                            # Include transformed fields too
                            column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                            select_items.append(f"{column_expr} AS {concept.concept_id}")
            else:
                # Single table query - safe to use SELECT *
                select_items.append("*")
        
        # Add DISTINCT for multi-table queries to avoid duplicates from JOINs
        # Especially important for customer_b with 1-to-many relationships
        distinct = ""
        if tables_needed and len(tables_needed) > 1 and not query_plan.aggregations:
            distinct = "DISTINCT "
        
        return f"SELECT {distinct}" + ", ".join(select_items)
    
    def _get_column_expression(
        self,
        mapping,
        customer_id: str,
        primary_table: Optional[str] = None
    ) -> str:
        """Get the column expression with transformations if needed.
        
        Args:
            mapping: ConceptMapping object
            customer_id: Customer identifier
            primary_table: Primary table name for the query (used in transformation references)
            
        Returns:
            Column expression (possibly with transformation)
        """
        table_prefix = self._get_table_alias(mapping.table_name)
        column_ref = f"{table_prefix}.{mapping.column_name}"
        
        # Apply transformation if specified
        if mapping.transformation:
            # Replace placeholders in transformation
            result = mapping.transformation.replace("{column}", column_ref)
            
            # For customer_b contract_status, replace 'id' with primary table reference
            if primary_table and "contract_id = id" in result:
                primary_alias = self._get_table_alias(primary_table)
                result = result.replace("contract_id = id", f"contract_id = {primary_alias}.id")
            
            return result
        
        return column_ref
    
    def _generate_from(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        tables_needed: Set[str]
    ) -> str:
        """Generate FROM clause with JOINs if needed.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            tables_needed: Set of tables required
            
        Returns:
            FROM clause with JOINs
        """
        if not tables_needed:
            raise ValueError("No tables identified for query")
        
        # Determine primary table (most frequently referenced or first in projections)
        primary_table = self._determine_primary_table(query_plan, customer_id, tables_needed)
        
        from_parts = [f"FROM {primary_table} AS {self._get_table_alias(primary_table)}"]
        
        # Handle multi-table queries (like Customer B)
        if len(tables_needed) > 1:
            # Add JOINs for additional tables
            for table in tables_needed:
                if table != primary_table:
                    join_clause = self._generate_join(primary_table, table, customer_id)
                    if join_clause:
                        from_parts.append(join_clause)
        
        return "\n".join(from_parts)
    
    def _determine_primary_table(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        tables_needed: Set[str]
    ) -> str:
        """Determine which table should be the primary table.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            tables_needed: Set of tables required
            
        Returns:
            Primary table name
        """
        # For single table, it's obvious
        if len(tables_needed) == 1:
            return list(tables_needed)[0]
        
        # For multi-table, prefer the first projection's table
        if query_plan.projections:
            first_concept = query_plan.projections[0]
            mapping = self.kg.get_mapping(first_concept, customer_id)
            if mapping and mapping.table_name in tables_needed:
                return mapping.table_name
        
        # Default to first table in sorted order
        return sorted(tables_needed)[0]
    
    def _generate_join(
        self,
        primary_table: str,
        join_table: str,
        customer_id: str
    ) -> Optional[str]:
        """Generate JOIN clause for a secondary table.
        
        Args:
            primary_table: Primary table name
            join_table: Table to join
            customer_id: Customer identifier
            
        Returns:
            JOIN clause or None if no join possible
        """
        # Customer B specific JOINs
        if customer_id == "customer_b":
            primary_alias = self._get_table_alias(primary_table)
            join_alias = self._get_table_alias(join_table)
            
            if primary_table == "contract_headers":
                if join_table == "renewal_schedule":
                    return f"JOIN {join_table} AS {join_alias} ON {primary_alias}.id = {join_alias}.contract_id"
                elif join_table == "contract_status_history":
                    return f"JOIN {join_table} AS {join_alias} ON {primary_alias}.id = {join_alias}.contract_id"
            
            elif primary_table == "renewal_schedule":
                if join_table == "contract_headers":
                    return f"JOIN {join_table} AS {join_alias} ON {primary_alias}.contract_id = {join_alias}.id"
            
            elif primary_table == "contract_status_history":
                if join_table == "contract_headers":
                    return f"JOIN {join_table} AS {join_alias} ON {primary_alias}.contract_id = {join_alias}.id"
        
        return None
    
    def _get_table_alias(self, table_name: str) -> str:
        """Get a short alias for a table name.
        
        Args:
            table_name: Full table name
            
        Returns:
            Table alias
        """
        # Simple aliases
        alias_map = {
            "contracts": "c",
            "contract_headers": "h",
            "contract_status_history": "s",
            "renewal_schedule": "r"
        }
        return alias_map.get(table_name, table_name[0])
    
    def _generate_where(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        primary_table: Optional[str] = None
    ) -> Optional[str]:
        """Generate WHERE clause from filters.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            primary_table: Primary table name for the query
            
        Returns:
            WHERE clause or None if no filters
        """
        if not query_plan.filters:
            return None
        
        conditions = []
        
        for filter in query_plan.filters:
            condition = self._compile_filter(filter, customer_id, primary_table)
            if condition:
                conditions.append(condition)
        
        if conditions:
            return "WHERE " + " AND ".join(conditions)
        
        return None
    
    def _compile_filter(
        self,
        filter: QueryFilter,
        customer_id: str,
        primary_table: Optional[str] = None
    ) -> str:
        """Compile a single filter to SQL condition.
        
        Args:
            filter: Query filter
            customer_id: Customer identifier
            primary_table: Primary table name for the query
            
        Returns:
            SQL condition
        """
        mapping = self.kg.get_mapping(filter.concept, customer_id)
        if not mapping:
            raise ValueError(f"No mapping for concept '{filter.concept}' in {customer_id}")
        
        column_expr = self._get_column_expression(mapping, customer_id, primary_table)
        
        # Handle different operators
        if filter.operator == QueryOperator.EQUALS:
            return f"{column_expr} = {self._quote_value(filter.value)}"
        
        elif filter.operator == QueryOperator.NOT_EQUALS:
            return f"{column_expr} != {self._quote_value(filter.value)}"
        
        elif filter.operator == QueryOperator.GREATER_THAN:
            return f"{column_expr} > {filter.value}"
        
        elif filter.operator == QueryOperator.GREATER_THAN_OR_EQUAL:
            return f"{column_expr} >= {filter.value}"
        
        elif filter.operator == QueryOperator.LESS_THAN:
            return f"{column_expr} < {filter.value}"
        
        elif filter.operator == QueryOperator.LESS_THAN_OR_EQUAL:
            return f"{column_expr} <= {filter.value}"
        
        elif filter.operator == QueryOperator.IN:
            values = ", ".join([self._quote_value(v) for v in filter.value])
            return f"{column_expr} IN ({values})"
        
        elif filter.operator == QueryOperator.CONTAINS:
            return f"{column_expr} LIKE {self._quote_value(f'%{filter.value}%')}"
        
        elif filter.operator == QueryOperator.WITHIN_NEXT_DAYS:
            # Handle date vs days_remaining
            if mapping.semantic_type == SemanticType.DAYS_REMAINING:
                return f"{column_expr} BETWEEN 0 AND {filter.value}"
            else:
                # Date comparison
                return f"{column_expr} BETWEEN CURRENT_DATE AND DATE(CURRENT_DATE, '+{filter.value} days')"
        
        elif filter.operator == QueryOperator.DATE_RANGE:
            # Expect filter.value to be a tuple (start, end)
            start, end = filter.value
            return f"{column_expr} BETWEEN {self._quote_value(start)} AND {self._quote_value(end)}"
        
        else:
            raise ValueError(f"Unsupported operator: {filter.operator}")
    
    def _generate_group_by(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        primary_table: Optional[str] = None
    ) -> Optional[str]:
        """Generate GROUP BY clause.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            primary_table: Primary table name for the query
            
        Returns:
            GROUP BY clause or None
        """
        if not query_plan.group_by:
            return None
        
        group_by_items = []
        for concept_id in query_plan.group_by:
            mapping = self.kg.get_mapping(concept_id, customer_id)
            if mapping:
                column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                group_by_items.append(column_expr)
        
        if group_by_items:
            return "GROUP BY " + ", ".join(group_by_items)
        
        return None
    
    def _generate_order_by(
        self,
        query_plan: SemanticQueryPlan,
        customer_id: str,
        primary_table: Optional[str] = None
    ) -> Optional[str]:
        """Generate ORDER BY clause.
        
        Args:
            query_plan: Semantic query plan
            customer_id: Customer identifier
            primary_table: Primary table name for the query
            
        Returns:
            ORDER BY clause or None
        """
        if not query_plan.order_by:
            return None
        
        order_items = []
        for concept_id, direction in query_plan.order_by:
            mapping = self.kg.get_mapping(concept_id, customer_id)
            if mapping:
                column_expr = self._get_column_expression(mapping, customer_id, primary_table)
                order_items.append(f"{column_expr} {direction.upper()}")
        
        if order_items:
            return "ORDER BY " + ", ".join(order_items)
        
        return None
    
    def _generate_limit(self, query_plan: SemanticQueryPlan) -> Optional[str]:
        """Generate LIMIT clause.
        
        Args:
            query_plan: Semantic query plan
            
        Returns:
            LIMIT clause or None
        """
        if query_plan.limit:
            return f"LIMIT {query_plan.limit}"
        return None
    
    def _quote_value(self, value) -> str:
        """Quote a value for SQL.
        
        Args:
            value: Value to quote
            
        Returns:
            Quoted value
        """
        if isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif value is None:
            return "NULL"
        else:
            return str(value)
