"""Validation script for Phase 1 completion."""

import json
import sqlite3
from pathlib import Path

from schema_translator.config import get_config
from schema_translator.models import (
    QueryFilter,
    QueryIntent,
    QueryOperator,
    QueryResult,
    SemanticQueryPlan,
)


def validate_databases():
    """Validate that all 6 databases were created with correct data."""
    config = get_config()
    print("🔍 Validating Phase 1 Databases...\n")
    
    customers = ["a", "b", "c", "d", "e", "f"]
    all_valid = True
    
    for customer in customers:
        db_path = config.get_database_path(customer)
        
        if not db_path.exists():
            print(f"❌ Customer {customer.upper()}: Database not found at {db_path}")
            all_valid = False
            continue
        
        # Connect and check
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Get table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                print(f"❌ Customer {customer.upper()}: No tables found")
                all_valid = False
                continue
            
            # Count total contracts
            if customer == "b":
                # Multi-table schema
                cursor.execute("SELECT COUNT(*) FROM contract_headers")
                count = cursor.fetchone()[0]
                table_info = f"{len(tables)} tables (contract_headers, contract_status_history, renewal_schedule)"
            else:
                # Single table schema
                cursor.execute("SELECT COUNT(*) FROM contracts")
                count = cursor.fetchone()[0]
                table_info = "1 table (contracts)"
            
            # Sample a few records
            if customer == "b":
                cursor.execute("SELECT contract_name, contract_value FROM contract_headers LIMIT 3")
            else:
                cursor.execute("SELECT * FROM contracts LIMIT 3")
            
            samples = cursor.fetchall()
            
            # Check for unique characteristics
            special_feature = ""
            if customer == "d":
                cursor.execute("PRAGMA table_info(contracts)")
                columns = [col[1] for col in cursor.fetchall()]
                if "days_remaining" in columns:
                    special_feature = " [days_remaining]"
            elif customer == "f":
                special_feature = " [ANNUAL values]"
            elif customer == "b":
                special_feature = " [normalized schema]"
            
            print(f"✅ Customer {customer.upper()}: {count} contracts, {table_info}{special_feature}")
            
        except Exception as e:
            print(f"❌ Customer {customer.upper()}: Error - {e}")
            all_valid = False
        finally:
            conn.close()
    
    print()
    if all_valid:
        print("✅ All databases validated successfully!")
    else:
        print("❌ Some databases have issues")
    
    return all_valid


def validate_models():
    """Validate that Pydantic models work correctly."""
    print("\n🔍 Validating Pydantic Models...\n")
    
    try:
        # Test SemanticQueryPlan serialization
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            filters=[
                QueryFilter(
                    concept="contract_expiration",
                    operator=QueryOperator.WITHIN_NEXT_DAYS,
                    value=30
                )
            ],
            projections=["contract_id", "contract_name", "contract_value"]
        )
        
        # Serialize to JSON
        json_str = plan.model_dump_json()
        print("✅ SemanticQueryPlan serialization works")
        
        # Deserialize from JSON
        plan_dict = json.loads(json_str)
        plan2 = SemanticQueryPlan(**plan_dict)
        assert plan2.intent == plan.intent
        print("✅ SemanticQueryPlan deserialization works")
        
        # Test QueryResult
        result = QueryResult(
            customer_id="customer_a",
            data=[{"id": 1, "name": "Test"}],
            sql_executed="SELECT * FROM contracts",
            execution_time_ms=10.5,
            row_count=1
        )
        
        assert result.success is True
        print("✅ QueryResult validation works")
        
        print("\n✅ All model validations passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Model validation failed: {e}")
        return False


def show_sample_data():
    """Show sample data from each customer database."""
    print("\n📊 Sample Data from Each Customer:\n")
    
    config = get_config()
    customers = {
        "a": "Customer A (Single table, DATE, LIFETIME)",
        "b": "Customer B (Multi-table, LIFETIME)",
        "c": "Customer C (Single table, different names, LIFETIME)",
        "d": "Customer D (Single table, days_remaining, LIFETIME)",
        "e": "Customer E (Single table, explicit term, LIFETIME)",
        "f": "Customer F (Single table, ANNUAL values)"
    }
    
    for customer_id, description in customers.items():
        print(f"\n{description}")
        print("=" * 70)
        
        db_path = config.get_database_path(customer_id)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            if customer_id == "b":
                # Multi-table query
                cursor.execute("""
                    SELECT h.contract_name, h.contract_value, r.renewal_date
                    FROM contract_headers h
                    JOIN renewal_schedule r ON h.id = r.contract_id
                    LIMIT 3
                """)
                rows = cursor.fetchall()
                for row in rows:
                    print(f"  {row[0]}: ${row[1]:,} (expires: {row[2]})")
            else:
                # Single table query
                cursor.execute("SELECT * FROM contracts LIMIT 3")
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    # Show key fields
                    id_field = row_dict.get('contract_id') or row_dict.get('id')
                    name_field = row_dict.get('contract_name') or row_dict.get('name') or row_dict.get('contract_title')
                    value_field = row_dict.get('contract_value') or row_dict.get('total_value')
                    
                    extra = ""
                    if 'days_remaining' in row_dict:
                        extra = f" (days_remaining: {row_dict['days_remaining']})"
                    elif 'term_years' in row_dict:
                        extra = f" (term: {row_dict['term_years']} years)"
                    
                    print(f"  {name_field}: ${value_field:,}{extra}")
        
        except Exception as e:
            print(f"  Error: {e}")
        finally:
            conn.close()


def main():
    """Run all Phase 1 validations."""
    print("=" * 70)
    print("PHASE 1 VALIDATION")
    print("=" * 70)
    
    db_valid = validate_databases()
    model_valid = validate_models()
    
    if db_valid and model_valid:
        show_sample_data()
        print("\n" + "=" * 70)
        print("✅ PHASE 1 COMPLETE - All validations passed!")
        print("=" * 70)
        print("\nNext: Start Phase 2 - Knowledge Graph implementation")
    else:
        print("\n" + "=" * 70)
        print("❌ PHASE 1 INCOMPLETE - Some validations failed")
        print("=" * 70)


if __name__ == "__main__":
    main()
