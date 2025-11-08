"""
Schema Drift Detector for monitoring database schema changes

This module monitors customer databases for schema changes that might
affect query execution and mappings.
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import json
import logging
from collections import defaultdict

from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.database_executor import DatabaseExecutor

logger = logging.getLogger(__name__)


class SchemaSnapshot:
    """Snapshot of a customer's database schema."""
    
    def __init__(
        self,
        customer_id: str,
        timestamp: datetime,
        tables: Dict[str, List[str]],
        row_counts: Dict[str, int]
    ):
        """Initialize schema snapshot.
        
        Args:
            customer_id: Customer identifier
            timestamp: When snapshot was taken
            tables: Dictionary of table_name -> [column_names]
            row_counts: Dictionary of table_name -> row_count
        """
        self.customer_id = customer_id
        self.timestamp = timestamp
        self.tables = tables
        self.row_counts = row_counts
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "customer_id": self.customer_id,
            "timestamp": self.timestamp.isoformat(),
            "tables": self.tables,
            "row_counts": self.row_counts
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaSnapshot':
        """Create from dictionary."""
        return cls(
            customer_id=data["customer_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            tables=data["tables"],
            row_counts=data["row_counts"]
        )


class SchemaDrift:
    """Represents detected schema drift."""
    
    def __init__(
        self,
        customer_id: str,
        drift_type: str,
        severity: str,
        description: str,
        details: Dict[str, Any]
    ):
        """Initialize schema drift.
        
        Args:
            customer_id: Customer identifier
            drift_type: Type of drift (table_added, table_removed, column_added, etc.)
            severity: Severity level (low, medium, high, critical)
            description: Human-readable description
            details: Additional details about the drift
        """
        self.customer_id = customer_id
        self.drift_type = drift_type
        self.severity = severity
        self.description = description
        self.details = details
        self.detected_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "customer_id": self.customer_id,
            "drift_type": self.drift_type,
            "severity": self.severity,
            "description": self.description,
            "details": self.details,
            "detected_at": self.detected_at.isoformat()
        }


class SchemaDriftDetector:
    """Monitors database schemas for changes."""
    
    def __init__(
        self,
        database_executor: DatabaseExecutor,
        knowledge_graph: SchemaKnowledgeGraph,
        snapshot_file: Optional[Path] = None
    ):
        """Initialize drift detector.
        
        Args:
            database_executor: Database executor for querying schemas
            knowledge_graph: Knowledge graph with mappings
            snapshot_file: File to store schema snapshots
        """
        self.executor = database_executor
        self.knowledge_graph = knowledge_graph
        self.snapshot_file = snapshot_file or Path("data/schema_snapshots.json")
        self.snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load previous snapshots
        self.snapshots: Dict[str, SchemaSnapshot] = {}
        self._load_snapshots()
        
        logger.info(f"SchemaDriftDetector initialized with {len(self.snapshots)} snapshots")
    
    def capture_snapshot(self, customer_id: str) -> SchemaSnapshot:
        """Capture current schema snapshot for a customer.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            SchemaSnapshot object
        """
        try:
            # Get database path from executor's config
            db_path = self.executor.config.get_database_path(customer_id)
            if not db_path.exists():
                raise FileNotFoundError(f"Database not found: {db_path}")
            
            # Connect and query schema
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            table_names = [row[0] for row in cursor.fetchall()]
            
            # Get columns for each table
            tables = {}
            row_counts = {}
            
            for table_name in table_names:
                # Get columns
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                tables[table_name] = columns
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_counts[table_name] = cursor.fetchone()[0]
            
            conn.close()
            
            snapshot = SchemaSnapshot(
                customer_id=customer_id,
                timestamp=datetime.now(timezone.utc),
                tables=tables,
                row_counts=row_counts
            )
            
            logger.info(f"Captured schema snapshot for {customer_id}: "
                       f"{len(tables)} tables, {sum(row_counts.values())} total rows")
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Error capturing snapshot for {customer_id}: {e}", exc_info=True)
            raise
    
    def detect_drift(
        self,
        customer_id: str,
        update_snapshot: bool = True
    ) -> List[SchemaDrift]:
        """Detect schema drift for a customer.
        
        Args:
            customer_id: Customer identifier
            update_snapshot: Whether to update stored snapshot after detection
            
        Returns:
            List of detected drifts
        """
        # Capture current snapshot
        current_snapshot = self.capture_snapshot(customer_id)
        
        # Get previous snapshot
        previous_snapshot = self.snapshots.get(customer_id)
        
        if not previous_snapshot:
            logger.info(f"No previous snapshot for {customer_id}, storing baseline")
            if update_snapshot:
                self.snapshots[customer_id] = current_snapshot
                self._save_snapshots()
            return []
        
        # Compare snapshots
        drifts = self._compare_snapshots(previous_snapshot, current_snapshot)
        
        # Update snapshot if requested
        if update_snapshot and drifts:
            self.snapshots[customer_id] = current_snapshot
            self._save_snapshots()
            logger.info(f"Detected {len(drifts)} drifts for {customer_id}, snapshot updated")
        
        return drifts
    
    def _compare_snapshots(
        self,
        old: SchemaSnapshot,
        new: SchemaSnapshot
    ) -> List[SchemaDrift]:
        """Compare two snapshots and detect drifts.
        
        Args:
            old: Previous snapshot
            new: Current snapshot
            
        Returns:
            List of detected drifts
        """
        drifts = []
        customer_id = new.customer_id
        
        old_tables = set(old.tables.keys())
        new_tables = set(new.tables.keys())
        
        # Check for added tables
        added_tables = new_tables - old_tables
        for table in added_tables:
            drifts.append(SchemaDrift(
                customer_id=customer_id,
                drift_type="table_added",
                severity="medium",
                description=f"New table '{table}' added with {len(new.tables[table])} columns",
                details={
                    "table_name": table,
                    "columns": new.tables[table],
                    "row_count": new.row_counts.get(table, 0)
                }
            ))
        
        # Check for removed tables
        removed_tables = old_tables - new_tables
        for table in removed_tables:
            # Check if this table was mapped
            is_mapped = self._is_table_mapped(customer_id, table)
            severity = "critical" if is_mapped else "high"
            
            drifts.append(SchemaDrift(
                customer_id=customer_id,
                drift_type="table_removed",
                severity=severity,
                description=f"Table '{table}' removed (was mapped: {is_mapped})",
                details={
                    "table_name": table,
                    "was_mapped": is_mapped,
                    "had_columns": old.tables[table]
                }
            ))
        
        # Check for column changes in existing tables
        common_tables = old_tables & new_tables
        for table in common_tables:
            old_cols = set(old.tables[table])
            new_cols = set(new.tables[table])
            
            # Added columns
            added_cols = new_cols - old_cols
            if added_cols:
                drifts.append(SchemaDrift(
                    customer_id=customer_id,
                    drift_type="columns_added",
                    severity="low",
                    description=f"Table '{table}': {len(added_cols)} columns added",
                    details={
                        "table_name": table,
                        "added_columns": list(added_cols)
                    }
                ))
            
            # Removed columns
            removed_cols = old_cols - new_cols
            if removed_cols:
                # Check if removed columns were mapped
                mapped_cols = self._get_mapped_columns(customer_id, table)
                affected_mappings = removed_cols & mapped_cols
                severity = "critical" if affected_mappings else "high"
                
                drifts.append(SchemaDrift(
                    customer_id=customer_id,
                    drift_type="columns_removed",
                    severity=severity,
                    description=f"Table '{table}': {len(removed_cols)} columns removed",
                    details={
                        "table_name": table,
                        "removed_columns": list(removed_cols),
                        "affected_mappings": list(affected_mappings)
                    }
                ))
        
        # Check for significant row count changes
        for table in common_tables:
            old_count = old.row_counts.get(table, 0)
            new_count = new.row_counts.get(table, 0)
            
            if old_count > 0:
                change_pct = abs(new_count - old_count) / old_count * 100
                
                if change_pct > 50:  # More than 50% change
                    drifts.append(SchemaDrift(
                        customer_id=customer_id,
                        drift_type="row_count_change",
                        severity="medium",
                        description=f"Table '{table}': significant row count change ({old_count} -> {new_count})",
                        details={
                            "table_name": table,
                            "old_count": old_count,
                            "new_count": new_count,
                            "change_percent": round(change_pct, 2)
                        }
                    ))
        
        return drifts
    
    def _is_table_mapped(self, customer_id: str, table_name: str) -> bool:
        """Check if a table is used in any mappings.
        
        Args:
            customer_id: Customer identifier
            table_name: Table name
            
        Returns:
            True if table is mapped
        """
        for concept in self.knowledge_graph.concepts.values():
            if customer_id in concept.customer_mappings:
                mapping = concept.customer_mappings[customer_id]
                if mapping.table == table_name:
                    return True
        return False
    
    def _get_mapped_columns(self, customer_id: str, table_name: str) -> Set[str]:
        """Get set of columns that are mapped for a table.
        
        Args:
            customer_id: Customer identifier
            table_name: Table name
            
        Returns:
            Set of mapped column names
        """
        mapped_cols = set()
        for concept in self.knowledge_graph.concepts.values():
            if customer_id in concept.customer_mappings:
                mapping = concept.customer_mappings[customer_id]
                if mapping.table == table_name:
                    mapped_cols.add(mapping.column)
        return mapped_cols
    
    def check_all_customers(self) -> Dict[str, List[SchemaDrift]]:
        """Check all customers for schema drift.
        
        Returns:
            Dictionary of customer_id -> list of drifts
        """
        all_drifts = {}
        
        # Get all customer databases from config
        database_dir = self.executor.config.database_dir
        if not database_dir.exists():
            logger.warning(f"Database directory not found: {database_dir}")
            return {}
        
        for db_file in database_dir.glob("*.db"):
            customer_id = db_file.stem
            try:
                drifts = self.detect_drift(customer_id, update_snapshot=True)
                if drifts:
                    all_drifts[customer_id] = drifts
            except Exception as e:
                logger.error(f"Error checking {customer_id}: {e}")
        
        return all_drifts
    
    def get_drift_summary(self) -> Dict[str, Any]:
        """Get summary of recent drift detections.
        
        Returns:
            Summary statistics
        """
        # Check all customers
        all_drifts = self.check_all_customers()
        
        if not all_drifts:
            return {
                "total_customers_checked": len(self.snapshots),
                "customers_with_drift": 0,
                "total_drifts": 0,
                "drifts_by_severity": {},
                "drifts_by_type": {},
                "critical_drifts": []
            }
        
        total_drifts = sum(len(drifts) for drifts in all_drifts.values())
        
        # Count by severity
        severity_counts = defaultdict(int)
        type_counts = defaultdict(int)
        critical_drifts = []
        
        for customer_id, drifts in all_drifts.items():
            for drift in drifts:
                severity_counts[drift.severity] += 1
                type_counts[drift.drift_type] += 1
                
                if drift.severity == "critical":
                    critical_drifts.append({
                        "customer_id": customer_id,
                        "type": drift.drift_type,
                        "description": drift.description
                    })
        
        return {
            "total_customers_checked": len(self.snapshots),
            "customers_with_drift": len(all_drifts),
            "total_drifts": total_drifts,
            "drifts_by_severity": dict(severity_counts),
            "drifts_by_type": dict(type_counts),
            "critical_drifts": critical_drifts
        }
    
    def _load_snapshots(self):
        """Load snapshots from disk."""
        if not self.snapshot_file.exists():
            return
        
        try:
            with open(self.snapshot_file, 'r') as f:
                data = json.load(f)
                for customer_id, snapshot_data in data.items():
                    self.snapshots[customer_id] = SchemaSnapshot.from_dict(snapshot_data)
        except Exception as e:
            logger.error(f"Error loading snapshots: {e}", exc_info=True)
    
    def _save_snapshots(self):
        """Save snapshots to disk."""
        try:
            data = {
                customer_id: snapshot.to_dict()
                for customer_id, snapshot in self.snapshots.items()
            }
            
            with open(self.snapshot_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving snapshots: {e}", exc_info=True)
