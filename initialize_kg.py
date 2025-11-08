"""Initialize the knowledge graph with all customer mappings."""

from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import SemanticType


def initialize_knowledge_graph() -> SchemaKnowledgeGraph:
    """Initialize and populate the knowledge graph with all mappings.
    
    Returns:
        Populated SchemaKnowledgeGraph instance
    """
    kg = SchemaKnowledgeGraph()
    
    print("🔧 Initializing Knowledge Graph...\n")
    
    # ========================================================================
    # 1. CONTRACT IDENTIFIER
    # ========================================================================
    print("  Adding concept: contract_identifier")
    kg.add_concept(
        concept_id="contract_identifier",
        concept_name="Contract Identifier",
        description="Unique identifier for a contract",
        aliases=["contract_id", "id", "contract_name", "name"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_a",
        table_name="contracts",
        column_name="contract_id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_b",
        table_name="contract_headers",
        column_name="id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_c",
        table_name="contracts",
        column_name="id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_d",
        table_name="contracts",
        column_name="contract_id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_e",
        table_name="contracts",
        column_name="contract_id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    kg.add_customer_mapping(
        concept_id="contract_identifier",
        customer_id="customer_f",
        table_name="contracts",
        column_name="contract_id",
        data_type="INTEGER",
        semantic_type=SemanticType.INTEGER
    )
    
    # ========================================================================
    # 2. CONTRACT EXPIRATION
    # ========================================================================
    print("  Adding concept: contract_expiration")
    kg.add_concept(
        concept_id="contract_expiration",
        concept_name="Contract Expiration",
        description="When the contract expires or is due for renewal",
        aliases=["expiry", "expiration", "renewal_date", "end_date"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_a",
        table_name="contracts",
        column_name="expiry_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_b",
        table_name="renewal_schedule",
        column_name="renewal_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE,
        join_requirements=["contract_headers"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_c",
        table_name="contracts",
        column_name="expiration_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_d",
        table_name="contracts",
        column_name="days_remaining",
        data_type="INTEGER",
        semantic_type=SemanticType.DAYS_REMAINING,
        transformation="DATE(CURRENT_DATE, '+' || days_remaining || ' days')"
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_e",
        table_name="contracts",
        column_name="expiry_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_expiration",
        customer_id="customer_f",
        table_name="contracts",
        column_name="expiration_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    # ========================================================================
    # 3. CONTRACT VALUE
    # ========================================================================
    print("  Adding concept: contract_value")
    kg.add_concept(
        concept_id="contract_value",
        concept_name="Contract Value",
        description="Monetary value of the contract",
        aliases=["value", "amount", "total_value", "contract_amount"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_a",
        table_name="contracts",
        column_name="contract_value",
        data_type="INTEGER",
        semantic_type=SemanticType.LIFETIME_TOTAL
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_b",
        table_name="contract_headers",
        column_name="contract_value",
        data_type="INTEGER",
        semantic_type=SemanticType.LIFETIME_TOTAL
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_c",
        table_name="contracts",
        column_name="total_value",
        data_type="INTEGER",
        semantic_type=SemanticType.LIFETIME_TOTAL
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_d",
        table_name="contracts",
        column_name="contract_value",
        data_type="INTEGER",
        semantic_type=SemanticType.LIFETIME_TOTAL
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_e",
        table_name="contracts",
        column_name="contract_value",
        data_type="INTEGER",
        semantic_type=SemanticType.LIFETIME_TOTAL
    )
    
    kg.add_customer_mapping(
        concept_id="contract_value",
        customer_id="customer_f",
        table_name="contracts",
        column_name="contract_value",
        data_type="INTEGER",
        semantic_type=SemanticType.ANNUAL_RECURRING_REVENUE,
        transformation="(contract_value * term_years)"
    )
    
    # ========================================================================
    # 4. CONTRACT STATUS
    # ========================================================================
    print("  Adding concept: contract_status")
    kg.add_concept(
        concept_id="contract_status",
        concept_name="Contract Status",
        description="Current status of the contract (active, inactive, expired, etc.)",
        aliases=["status", "current_status", "state"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_a",
        table_name="contracts",
        column_name="status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_b",
        table_name="contract_status_history",
        column_name="status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT,
        join_requirements=["contract_headers"],
        transformation="(SELECT status FROM contract_status_history WHERE contract_id = id ORDER BY status_date DESC LIMIT 1)"
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_c",
        table_name="contracts",
        column_name="current_status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_d",
        table_name="contracts",
        column_name="status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_e",
        table_name="contracts",
        column_name="status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    kg.add_customer_mapping(
        concept_id="contract_status",
        customer_id="customer_f",
        table_name="contracts",
        column_name="status",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    # ========================================================================
    # 5. CONTRACT START
    # ========================================================================
    # NOTE: industry_sector and customer_name concepts were removed.
    # - customer_name: Refers to company names in contracts (e.g., "Global Tech Inc")
    #   which conflicts with database selection (customer_a, customer_b, etc.)
    # - industry_sector: Internal attribute that doesn't represent typical
    #   business queries users would make.
    # ========================================================================
    print("  Adding concept: contract_start")
    kg.add_concept(
        concept_id="contract_start",
        concept_name="Contract Start Date",
        description="When the contract began or was signed",
        aliases=["start_date", "inception_date", "begin_date", "effective_date"]
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_a",
        table_name="contracts",
        column_name="start_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_b",
        table_name="contract_headers",
        column_name="start_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_c",
        table_name="contracts",
        column_name="inception_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_d",
        table_name="contracts",
        column_name="start_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_e",
        table_name="contracts",
        column_name="start_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    kg.add_customer_mapping(
        concept_id="contract_start",
        customer_id="customer_f",
        table_name="contracts",
        column_name="start_date",
        data_type="TEXT",
        semantic_type=SemanticType.DATE
    )
    
    # ========================================================================
    # TRANSFORMATION RULES
    # ========================================================================
    print("\n  Adding transformation rules...")
    
    # Days remaining to date
    kg.add_transformation(
        from_type="days_remaining",
        to_type="date",
        transformation_sql="DATE(CURRENT_DATE, '+' || {column} || ' days')"
    )
    
    # Date to days remaining
    kg.add_transformation(
        from_type="date",
        to_type="days_remaining",
        transformation_sql="CAST((JULIANDAY({column}) - JULIANDAY(CURRENT_DATE)) AS INTEGER)"
    )
    
    # Annual to lifetime
    kg.add_transformation(
        from_type="annual",
        to_type="lifetime",
        transformation_sql="({column} * {term_years_column})"
    )
    
    # Lifetime to annual
    kg.add_transformation(
        from_type="lifetime",
        to_type="annual",
        transformation_sql="({column} / {term_years_column})"
    )
    
    print("\n✅ Knowledge graph initialized successfully!")
    
    return kg


def main():
    """Main entry point for initializing the knowledge graph."""
    # Initialize
    kg = initialize_knowledge_graph()
    
    # Validate
    print("\n🔍 Validating knowledge graph...")
    validation = kg.validate()
    
    if validation["valid"]:
        print("✅ Validation passed!")
    else:
        print("⚠️  Validation issues found:")
        for issue in validation["issues"]:
            print(f"  - {issue}")
    
    if validation["warnings"]:
        print("\n⚠️  Warnings:")
        for warning in validation["warnings"]:
            print(f"  - {warning}")
    
    # Show stats
    print("\n📊 Knowledge Graph Statistics:")
    stats = kg.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Save
    print("\n💾 Saving knowledge graph...")
    kg.save()
    print(f"✅ Saved to: {kg.config.knowledge_graph_path}")
    
    # Test loading
    print("\n🔄 Testing load...")
    kg2 = SchemaKnowledgeGraph()
    kg2.load()
    print(f"✅ Loaded successfully: {kg2}")


if __name__ == "__main__":
    main()
