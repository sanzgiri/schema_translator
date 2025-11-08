"""
Tests for Phase 8: Learning and Polish

Tests for FeedbackLoop and SchemaDriftDetector components.
"""

import pytest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from schema_translator.feedback_loop import FeedbackLoop
from schema_translator.schema_drift_detector import (
    SchemaDriftDetector,
    SchemaSnapshot,
    SchemaDrift
)
from schema_translator.models import SemanticQueryPlan, QueryIntent
from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.database_executor import DatabaseExecutor


class TestFeedbackLoop:
    """Test feedback loop functionality."""
    
    def test_initialization(self, tmp_path):
        """Test feedback loop initialization."""
        feedback_file = tmp_path / "feedback.jsonl"
        loop = FeedbackLoop(feedback_file=feedback_file)
        
        assert loop.feedback_file == feedback_file
        assert len(loop.feedback_cache) == 0
        assert len(loop.query_patterns) == 0
    
    def test_submit_good_feedback(self, tmp_path):
        """Test submitting positive feedback."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        feedback = loop.submit_feedback(
            query_text="Show contracts",
            semantic_plan=plan,
            feedback_type="good"
        )
        
        assert feedback.feedback_type == "good"
        assert len(loop.feedback_cache) == 1
        assert "find_contracts" in loop.query_patterns
    
    def test_submit_negative_feedback(self, tmp_path):
        """Test submitting negative feedback."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        feedback = loop.submit_feedback(
            query_text="Show bad query",
            semantic_plan=plan,
            feedback_type="incorrect",
            feedback_text="Results were wrong"
        )
        
        assert feedback.feedback_type == "incorrect"
        assert len(loop.failure_patterns["incorrect"]) == 1
    
    def test_get_feedback_summary(self, tmp_path):
        """Test getting feedback summary."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        # Submit multiple feedbacks
        loop.submit_feedback("query1", plan, "good")
        loop.submit_feedback("query2", plan, "good")
        loop.submit_feedback("query3", plan, "incorrect")
        
        summary = loop.get_feedback_summary(days=30)
        
        assert summary["total_feedback"] == 3
        assert summary["feedback_types"]["good"] == 2
        assert summary["feedback_types"]["incorrect"] == 1
        assert summary["success_rate"] == pytest.approx(66.67, rel=0.1)
    
    def test_analyze_failure_patterns(self, tmp_path):
        """Test failure pattern analysis."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        # Submit some failures
        loop.submit_feedback("find contracts", plan, "incorrect")
        loop.submit_feedback("find contracts", plan, "incorrect")
        loop.submit_feedback("show contracts value", plan, "missing")
        
        analysis = loop.analyze_failure_patterns()
        
        assert analysis["total_failures"] == 3
        assert analysis["unique_failures"] == 2
        assert len(analysis["common_terms"]) > 0
    
    def test_suggest_new_concepts(self, tmp_path):
        """Test new concept suggestions."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        # Submit failures with repeated terms
        for _ in range(3):
            loop.submit_feedback("show customer revenue", plan, "missing")
            loop.submit_feedback("find customer profit", plan, "missing")
        
        suggestions = loop.suggest_new_concepts(min_occurrences=2)
        
        assert len(suggestions) > 0
        # Should suggest "customer", "revenue", "profit"
        terms = [s["term"] for s in suggestions]
        assert "customer" in terms or "revenue" in terms
    
    def test_persistence(self, tmp_path):
        """Test feedback persistence."""
        feedback_file = tmp_path / "feedback.jsonl"
        
        # Create and populate feedback loop
        loop1 = FeedbackLoop(feedback_file=feedback_file)
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        loop1.submit_feedback("query1", plan, "good")
        loop1.submit_feedback("query2", plan, "incorrect")
        
        # Create new loop from same file
        loop2 = FeedbackLoop(feedback_file=feedback_file)
        
        assert len(loop2.feedback_cache) == 2
        assert len(loop2.query_patterns) > 0
    
    def test_clear_old_feedback(self, tmp_path):
        """Test clearing old feedback."""
        loop = FeedbackLoop(feedback_file=tmp_path / "feedback.jsonl")
        
        plan = SemanticQueryPlan(
            intent=QueryIntent.FIND_CONTRACTS,
            projections=["contract_id"],
            filters=[],
            aggregations=[]
        )
        
        # Add some feedback
        loop.submit_feedback("query1", plan, "good")
        
        # Manually set old timestamp
        loop.feedback_cache[0].timestamp = datetime.now(timezone.utc) - timedelta(days=100)
        
        removed = loop.clear_old_feedback(days=90)
        
        assert removed == 1
        assert len(loop.feedback_cache) == 0


class TestSchemaSnapshot:
    """Test schema snapshot functionality."""
    
    def test_create_snapshot(self):
        """Test creating a schema snapshot."""
        snapshot = SchemaSnapshot(
            customer_id="test_customer",
            timestamp=datetime.now(timezone.utc),
            tables={"contracts": ["id", "value", "status"]},
            row_counts={"contracts": 10}
        )
        
        assert snapshot.customer_id == "test_customer"
        assert "contracts" in snapshot.tables
        assert snapshot.row_counts["contracts"] == 10
    
    def test_snapshot_serialization(self):
        """Test snapshot to/from dict."""
        snapshot1 = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"t1": ["c1", "c2"]},
            row_counts={"t1": 5}
        )
        
        data = snapshot1.to_dict()
        snapshot2 = SchemaSnapshot.from_dict(data)
        
        assert snapshot1.customer_id == snapshot2.customer_id
        assert snapshot1.tables == snapshot2.tables
        assert snapshot1.row_counts == snapshot2.row_counts


class TestSchemaDriftDetector:
    """Test schema drift detection."""
    
    @pytest.fixture
    def knowledge_graph(self):
        """Create test knowledge graph."""
        kg = SchemaKnowledgeGraph()
        return kg
    
    @pytest.fixture
    def executor(self):
        """Create test database executor."""
        return DatabaseExecutor()
    
    @pytest.fixture
    def detector(self, executor, knowledge_graph, tmp_path):
        """Create drift detector."""
        snapshot_file = tmp_path / "snapshots.json"
        return SchemaDriftDetector(
            executor,
            knowledge_graph,
            snapshot_file=snapshot_file
        )
    
    def test_initialization(self, detector):
        """Test detector initialization."""
        assert detector.executor is not None
        assert detector.knowledge_graph is not None
        assert len(detector.snapshots) == 0
    
    def test_capture_snapshot(self, detector):
        """Test capturing a schema snapshot."""
        # This test requires actual database
        try:
            snapshot = detector.capture_snapshot("customer_a")
            assert snapshot.customer_id == "customer_a"
            assert len(snapshot.tables) > 0
        except FileNotFoundError:
            pytest.skip("Test database not found")
    
    def test_detect_table_added(self, detector):
        """Test detecting added table."""
        # Create old snapshot
        old_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        # Create new snapshot with added table
        new_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={
                "table1": ["col1"],
                "table2": ["col2", "col3"]
            },
            row_counts={"table1": 10, "table2": 5}
        )
        
        drifts = detector._compare_snapshots(old_snapshot, new_snapshot)
        
        assert len(drifts) == 1
        assert drifts[0].drift_type == "table_added"
        assert drifts[0].details["table_name"] == "table2"
    
    def test_detect_table_removed(self, detector):
        """Test detecting removed table."""
        old_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"], "table2": ["col2"]},
            row_counts={"table1": 10, "table2": 5}
        )
        
        new_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        drifts = detector._compare_snapshots(old_snapshot, new_snapshot)
        
        assert len(drifts) == 1
        assert drifts[0].drift_type == "table_removed"
        assert drifts[0].details["table_name"] == "table2"
    
    def test_detect_columns_added(self, detector):
        """Test detecting added columns."""
        old_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        new_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1", "col2", "col3"]},
            row_counts={"table1": 10}
        )
        
        drifts = detector._compare_snapshots(old_snapshot, new_snapshot)
        
        assert len(drifts) == 1
        assert drifts[0].drift_type == "columns_added"
        assert len(drifts[0].details["added_columns"]) == 2
    
    def test_detect_columns_removed(self, detector):
        """Test detecting removed columns."""
        old_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1", "col2", "col3"]},
            row_counts={"table1": 10}
        )
        
        new_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        drifts = detector._compare_snapshots(old_snapshot, new_snapshot)
        
        assert len(drifts) == 1
        assert drifts[0].drift_type == "columns_removed"
        assert len(drifts[0].details["removed_columns"]) == 2
    
    def test_detect_row_count_change(self, detector):
        """Test detecting significant row count changes."""
        old_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 100}
        )
        
        new_snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}  # 90% decrease
        )
        
        drifts = detector._compare_snapshots(old_snapshot, new_snapshot)
        
        assert len(drifts) == 1
        assert drifts[0].drift_type == "row_count_change"
        assert drifts[0].details["change_percent"] > 50
    
    def test_no_drift_detected(self, detector):
        """Test when no drift exists."""
        snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        drifts = detector._compare_snapshots(snapshot, snapshot)
        
        assert len(drifts) == 0
    
    def test_snapshot_persistence(self, detector, tmp_path):
        """Test snapshot persistence."""
        snapshot = SchemaSnapshot(
            customer_id="test",
            timestamp=datetime.now(timezone.utc),
            tables={"table1": ["col1"]},
            row_counts={"table1": 10}
        )
        
        detector.snapshots["test"] = snapshot
        detector._save_snapshots()
        
        # Create new detector
        detector2 = SchemaDriftDetector(
            detector.executor,
            detector.knowledge_graph,
            snapshot_file=detector.snapshot_file
        )
        
        assert "test" in detector2.snapshots
        assert detector2.snapshots["test"].customer_id == "test"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
