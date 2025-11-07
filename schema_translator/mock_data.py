"""Mock data generation for customer contract databases."""

import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from schema_translator.config import get_config


class MockDataGenerator:
    """Generates realistic contract databases for multiple customers."""
    
    # Industry variations across customers
    INDUSTRIES = {
        "customer_a": ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail"],
        "customer_b": ["Tech", "Medical", "Financial Services", "Industrial", "E-commerce"],
        "customer_c": ["Information Technology", "Health Services", "Banking", "Manufacturing", "Retail"],
        "customer_d": ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail"],
        "customer_e": ["Tech", "Healthcare", "Financial", "Manufacturing", "Retail"],
        "customer_f": ["Technology", "Medical", "Finance", "Manufacturing", "E-commerce"],
    }
    
    # Company name prefixes/suffixes
    COMPANY_PREFIXES = ["Global", "Premier", "Advanced", "United", "National", "International"]
    COMPANY_CORES = ["Tech", "Systems", "Solutions", "Industries", "Group", "Corp", "Enterprises"]
    COMPANY_SUFFIXES = ["Inc", "LLC", "Ltd", "Corporation", "Partners", "Holdings"]
    
    # Status values
    STATUSES = ["active", "inactive", "expired", "pending"]
    
    def __init__(self):
        """Initialize the mock data generator."""
        self.config = get_config()
        self.config.database_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_company_name(self) -> str:
        """Generate a realistic company name."""
        if random.random() < 0.3:
            # Simple format: "CoreName Suffix"
            return f"{random.choice(self.COMPANY_CORES)} {random.choice(self.COMPANY_SUFFIXES)}"
        else:
            # Full format: "Prefix CoreName Suffix"
            return f"{random.choice(self.COMPANY_PREFIXES)} {random.choice(self.COMPANY_CORES)} {random.choice(self.COMPANY_SUFFIXES)}"
    
    def generate_contract_name(self, contract_id: int) -> str:
        """Generate a contract name/identifier."""
        prefixes = ["CONTRACT", "AGR", "SVC", "MSA", "SOW"]
        return f"{random.choice(prefixes)}-{contract_id:04d}"
    
    def generate_dates(self) -> Tuple[datetime, datetime]:
        """Generate start and expiry dates for a contract.
        
        Returns:
            Tuple of (start_date, expiry_date)
        """
        # Start dates: 1-3 years ago
        days_ago = random.randint(365, 1095)
        start_date = datetime.now() - timedelta(days=days_ago)
        
        # Contract duration: 1-3 years
        contract_duration_days = random.randint(365, 1095)
        expiry_date = start_date + timedelta(days=contract_duration_days)
        
        # Some contracts should be expired (past), some upcoming (future)
        # Mix: 20% expired, 80% active or future
        if random.random() < 0.2:
            # Make it expired (subtract extra time)
            extra_days = random.randint(1, 60)
            expiry_date = datetime.now() - timedelta(days=extra_days)
        
        return start_date, expiry_date
    
    def generate_contract_value(self, is_annual: bool = False) -> int:
        """Generate contract value.
        
        Args:
            is_annual: If True, generate annual value (ARR), else lifetime value
            
        Returns:
            Contract value in dollars
        """
        if is_annual:
            # Annual: $100K - $2M
            return random.randint(100_000, 2_000_000)
        else:
            # Lifetime: $100K - $5M
            return random.randint(100_000, 5_000_000)
    
    def generate_all_databases(self):
        """Generate all 6 customer databases."""
        print("🏗️  Generating mock customer databases...\n")
        
        self.generate_customer_a()
        self.generate_customer_b()
        self.generate_customer_c()
        self.generate_customer_d()
        self.generate_customer_e()
        self.generate_customer_f()
        
        print("\n✅ All databases generated successfully!")
        print(f"📁 Databases located in: {self.config.database_dir}")
    
    def generate_customer_a(self):
        """Generate Customer A: Single table, DATE expiry, LIFETIME contract_value."""
        db_path = self.config.get_database_path("customer_a")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id INTEGER PRIMARY KEY,
                contract_name TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                contract_value INTEGER NOT NULL,
                status TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                start_date TEXT NOT NULL,
                industry TEXT NOT NULL
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_a"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            # Determine status based on expiry
            if expiry_date < datetime.now():
                status = "expired"
            else:
                status = random.choice(["active", "active", "active", "inactive"])
            
            cursor.execute("""
                INSERT INTO contracts 
                (contract_id, contract_name, customer_name, contract_value, status, 
                 expiry_date, start_date, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=False),
                status,
                expiry_date.strftime("%Y-%m-%d"),
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer A: 50 contracts generated ({db_path})")
    
    def generate_customer_b(self):
        """Generate Customer B: Normalized (3 tables), LIFETIME value."""
        db_path = self.config.get_database_path("customer_b")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contract_headers (
                id INTEGER PRIMARY KEY,
                contract_name TEXT NOT NULL,
                client_name TEXT NOT NULL,
                contract_value INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                sector TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contract_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                status_date TEXT NOT NULL,
                FOREIGN KEY (contract_id) REFERENCES contract_headers(id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS renewal_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id INTEGER NOT NULL,
                renewal_date TEXT NOT NULL,
                FOREIGN KEY (contract_id) REFERENCES contract_headers(id)
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_b"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            # Insert header
            cursor.execute("""
                INSERT INTO contract_headers 
                (id, contract_name, client_name, contract_value, start_date, sector)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=False),
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
            
            # Insert status history (1-3 status changes)
            num_status_changes = random.randint(1, 3)
            current_date = start_date
            for j in range(num_status_changes):
                if j == num_status_changes - 1 and expiry_date < datetime.now():
                    status = "expired"
                    status_date = expiry_date
                else:
                    status = random.choice(["active", "active", "inactive", "pending"])
                    status_date = current_date
                
                cursor.execute("""
                    INSERT INTO contract_status_history 
                    (contract_id, status, status_date)
                    VALUES (?, ?, ?)
                """, (i, status, status_date.strftime("%Y-%m-%d")))
                
                current_date += timedelta(days=random.randint(30, 180))
            
            # Insert renewal date
            cursor.execute("""
                INSERT INTO renewal_schedule (contract_id, renewal_date)
                VALUES (?, ?)
            """, (i, expiry_date.strftime("%Y-%m-%d")))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer B: 50 contracts generated with 3 tables ({db_path})")
    
    def generate_customer_c(self):
        """Generate Customer C: Single table, DATE expiry, LIFETIME value, different column names."""
        db_path = self.config.get_database_path("customer_c")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table with different column names
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                account TEXT NOT NULL,
                total_value INTEGER NOT NULL,
                current_status TEXT NOT NULL,
                expiration_date TEXT NOT NULL,
                inception_date TEXT NOT NULL,
                business_sector TEXT NOT NULL
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_c"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            if expiry_date < datetime.now():
                status = "expired"
            else:
                status = random.choice(["active", "active", "active", "inactive"])
            
            cursor.execute("""
                INSERT INTO contracts 
                (id, name, account, total_value, current_status, 
                 expiration_date, inception_date, business_sector)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=False),
                status,
                expiry_date.strftime("%Y-%m-%d"),
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer C: 50 contracts generated ({db_path})")
    
    def generate_customer_d(self):
        """Generate Customer D: Single table, INTEGER days_remaining, LIFETIME value."""
        db_path = self.config.get_database_path("customer_d")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table with days_remaining instead of date
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id INTEGER PRIMARY KEY,
                contract_title TEXT NOT NULL,
                customer_org TEXT NOT NULL,
                contract_value INTEGER NOT NULL,
                status TEXT NOT NULL,
                days_remaining INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                industry TEXT NOT NULL
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_d"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            # Calculate days remaining from now
            days_remaining = (expiry_date - datetime.now()).days
            
            if days_remaining < 0:
                status = "expired"
            else:
                status = random.choice(["active", "active", "active", "inactive"])
            
            cursor.execute("""
                INSERT INTO contracts 
                (contract_id, contract_title, customer_org, contract_value, status, 
                 days_remaining, start_date, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=False),
                status,
                days_remaining,
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer D: 50 contracts generated (using days_remaining) ({db_path})")
    
    def generate_customer_e(self):
        """Generate Customer E: Single table, DATE expiry, LIFETIME value with explicit duration."""
        db_path = self.config.get_database_path("customer_e")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id INTEGER PRIMARY KEY,
                contract_name TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                contract_value INTEGER NOT NULL,
                term_years REAL NOT NULL,
                status TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                start_date TEXT NOT NULL,
                industry TEXT NOT NULL
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_e"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            # Calculate term in years
            term_days = (expiry_date - start_date).days
            term_years = round(term_days / 365.0, 1)
            
            if expiry_date < datetime.now():
                status = "expired"
            else:
                status = random.choice(["active", "active", "active", "inactive"])
            
            cursor.execute("""
                INSERT INTO contracts 
                (contract_id, contract_name, customer_name, contract_value, term_years,
                 status, expiry_date, start_date, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=False),
                term_years,
                status,
                expiry_date.strftime("%Y-%m-%d"),
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer E: 50 contracts generated ({db_path})")
    
    def generate_customer_f(self):
        """Generate Customer F: Single table, DATE expiry, ANNUAL contract_value (ARR)."""
        db_path = self.config.get_database_path("customer_f")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                account TEXT NOT NULL,
                contract_value INTEGER NOT NULL,
                term_years REAL NOT NULL,
                status TEXT NOT NULL,
                expiration_date TEXT NOT NULL,
                start_date TEXT NOT NULL,
                sector TEXT NOT NULL
            )
        """)
        
        # Generate data
        industries = self.INDUSTRIES["customer_f"]
        for i in range(1, 51):
            start_date, expiry_date = self.generate_dates()
            
            # Calculate term in years
            term_days = (expiry_date - start_date).days
            term_years = round(term_days / 365.0, 1)
            
            if expiry_date < datetime.now():
                status = "expired"
            else:
                status = random.choice(["active", "active", "active", "inactive"])
            
            cursor.execute("""
                INSERT INTO contracts 
                (contract_id, name, account, contract_value, term_years,
                 status, expiration_date, start_date, sector)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                i,
                self.generate_contract_name(i),
                self.generate_company_name(),
                self.generate_contract_value(is_annual=True),  # ANNUAL value
                term_years,
                status,
                expiry_date.strftime("%Y-%m-%d"),
                start_date.strftime("%Y-%m-%d"),
                random.choice(industries)
            ))
        
        conn.commit()
        conn.close()
        print(f"✓ Customer F: 50 contracts generated (ANNUAL values) ({db_path})")


def main():
    """Main entry point for generating mock databases."""
    generator = MockDataGenerator()
    generator.generate_all_databases()


if __name__ == "__main__":
    main()
