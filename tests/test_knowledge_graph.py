"""Tests for the knowledge graph."""

import json
from pathlib import Path

import pytest

from schema_translator.knowledge_graph import SchemaKnowledgeGraph
from schema_translator.models import SemanticType


@pytest.fixture
def empty_kg():
    """Create an empty knowledge graph."""
    return SchemaKnowledgeGraph()


@pytest.fixture
def populated_kg():
    """Create a populated knowledge graph with test data."""
    kg = SchemaKnowledgeGraph()
    
    # Add a concept
    kg.add_concept(
        concept_id="test_concept",
        concept_name="Test Concept",
        description="A test concept",
        aliases=["test", "example"]
    )
    
    # Add mappings
    kg.add_customer_mapping(
        concept_id="test_concept",
        customer_id="customer_a",
        table_name="test_table",
        column_name="test_column",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    kg.add_customer_mapping(
        concept_id="test_concept",
        customer_id="customer_b",
        table_name="test_table",
        column_name="test_col",
        data_type="TEXT",
        semantic_type=SemanticType.TEXT
    )
    
    # Add transformation
    kg.add_transformation(
        from_type="type_a",
        to_type="type_b",
        transformation_sql="TRANSFORM({column})"
    )
    
    return kg


class TestKnowledgeGraphBasics:
    """Test basic knowledge graph operations."""
    
    def test_create_empty_graph(self, empty_kg):
        """Test creating an empty knowledge graph."""
        assert len(empty_kg.concepts) == 0
        assert empty_kg.graph.number_of_nodes() == 0
    
    def test_add_concept(self, empty_kg):
        """Test adding a concept."""
        concept = empty_kg.add_concept(
            concept_id="contract_id",
            concept_name="Contract ID",
            description="Contract identifier",
            aliases=["id"]
        )
        
        assert concept.concept_id == "contract_id"
        assert len(empty_kg.concepts) == 1
        assert empty_kg.graph.number_of_nodes() == 1
    
    def test_add_customer_mapping(self, empty_kg):
        """Test adding a customer mapping."""
        empty_kg.add_concept(
            concept_id="contract_id",
            concept_name="Contract ID",
            description="Contract identifier"
        )
        
        empty_kg.add_customer_mapping(
            concept_id="contract_id",
            customer_id="customer_a",
            table_name="contracts",
            column_name="id",
            data_type="INTEGER",
            semantic_type=SemanticType.INTEGER
        )
        
        mapping = empty_kg.get_mapping("contract_id", "customer_a")
        assert mapping is not None
        assert mapping.table_name == "contracts"
        assert mapping.column_name == "id"
    
    def test_add_mapping_to_nonexistent_concept(self, empty_kg):
        """Test that adding a mapping to a nonexistent concept raises an error."""
        with pytest.raises(ValueError, match="Concept .* not found"):
            empty_kg.add_customer_mapping(
                concept_id="nonexistent",
                customer_id="customer_a",
                table_name="table",
                column_name="column",
                data_type="TEXT",
                semantic_type=SemanticType.TEXT
            )
    
    def test_get_concept(self, populated_kg):
        """Test retrieving a concept."""
        concept = populated_kg.get_concept("test_concept")
        assert concept is not None
        assert concept.concept_name == "Test Concept"
        
        nonexistent = populated_kg.get_concept("nonexistent")
        assert nonexistent is None
    
    def test_get_mapping(self, populated_kg):
        """Test retrieving a mapping."""
        mapping = populated_kg.get_mapping("test_concept", "customer_a")
        assert mapping is not None
        assert mapping.column_name == "test_column"
        
        nonexistent = populated_kg.get_mapping("test_concept", "customer_z")
        assert nonexistent is None


class TestTransformations:
    """Test transformation rules."""
    
    def test_add_transformation(self, empty_kg):
        """Test adding a transformation rule."""
        empty_kg.add_transformation(
            from_type="days",
            to_type="date",
            transformation_sql="DATE_ADD({column})"
        )
        
        transform = empty_kg.get_transformation("days", "date")
        assert transform == "DATE_ADD({column})"
    
    def test_get_nonexistent_transformation(self, empty_kg):
        """Test getting a nonexistent transformation."""
        transform = empty_kg.get_transformation("type_a", "type_b")
        assert transform is None
    
    def test_multiple_transformations(self, populated_kg):
        """Test multiple transformation rules."""
        assert populated_kg.get_transformation("type_a", "type_b") is not None


class TestSearch:
    """Test search and lookup functionality."""
    
    def test_find_concept_by_id(self, populated_kg):
        """Test finding concept by ID."""
        concept = populated_kg.find_concept_by_alias("test_concept")
        assert concept is not None
        assert concept.concept_id == "test_concept"
    
    def test_find_concept_by_name(self, populated_kg):
        """Test finding concept by name."""
        concept = populated_kg.find_concept_by_alias("Test Concept")
        assert concept is not None
        assert concept.concept_id == "test_concept"
    
    def test_find_concept_by_alias(self, populated_kg):
        """Test finding concept by alias."""
        concept = populated_kg.find_concept_by_alias("test")
        assert concept is not None
        assert concept.concept_id == "test_concept"
    
    def test_find_nonexistent_concept(self, populated_kg):
        """Test finding a nonexistent concept."""
        concept = populated_kg.find_concept_by_alias("nonexistent")
        assert concept is None
    
    def test_get_all_concepts(self, populated_kg):
        """Test getting all concepts."""
        concepts = populated_kg.get_all_concepts()
        assert len(concepts) == 1
        assert concepts[0].concept_id == "test_concept"
    
    def test_get_customers_for_concept(self, populated_kg):
        """Test getting customers for a concept."""
        customers = populated_kg.get_customers_for_concept("test_concept")
        assert len(customers) == 2
        assert "customer_a" in customers
        assert "customer_b" in customers


class TestPersistence:
    """Test save and load functionality."""
    
    def test_save_and_load(self, populated_kg, tmp_path):
        """Test saving and loading the knowledge graph."""
        # Save to temporary path
        temp_file = tmp_path / "test_kg.json"
        populated_kg.save(temp_file)
        
        assert temp_file.exists()
        
        # Load into new graph
        new_kg = SchemaKnowledgeGraph()
        new_kg.load(temp_file)
        
        # Verify content
        assert len(new_kg.concepts) == len(populated_kg.concepts)
        assert new_kg.get_concept("test_concept") is not None
        assert new_kg.get_mapping("test_concept", "customer_a") is not None
    
    def test_save_json_format(self, populated_kg, tmp_path):
        """Test that saved JSON has correct format."""
        temp_file = tmp_path / "test_kg.json"
        populated_kg.save(temp_file)
        
        with open(temp_file) as f:
            data = json.load(f)
        
        assert "concepts" in data
        assert "transformations" in data
        assert "test_concept" in data["concepts"]
    
    def test_load_nonexistent_file(self, empty_kg, tmp_path):
        """Test loading a nonexistent file raises an error."""
        nonexistent = tmp_path / "nonexistent.json"
        
        with pytest.raises(FileNotFoundError):
            empty_kg.load(nonexistent)


class TestValidation:
    """Test validation functionality."""
    
    def test_validate_empty_graph(self, empty_kg):
        """Test validating an empty graph."""
        result = empty_kg.validate()
        assert result["valid"] is True
        assert result["concepts_count"] == 0
    
    def test_validate_populated_graph(self, populated_kg):
        """Test validating a populated graph."""
        result = populated_kg.validate()
        assert "valid" in result
        assert "concepts_count" in result
        assert "customers_count" in result
    
    def test_get_stats(self, populated_kg):
        """Test getting graph statistics."""
        stats = populated_kg.get_stats()
        
        assert stats["total_concepts"] == 1
        assert stats["total_customers"] == 2
        assert stats["total_mappings"] == 2
        assert stats["total_transformations"] == 1


class TestLoadedKnowledgeGraph:
    """Test the actual loaded knowledge graph."""
    
    def test_load_initialized_graph(self):
        """Test loading the initialized knowledge graph."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        # Check concepts exist
        assert len(kg.concepts) == 7
        assert kg.get_concept("contract_identifier") is not None
        assert kg.get_concept("contract_value") is not None
        assert kg.get_concept("contract_expiration") is not None
    
    def test_all_customers_mapped(self):
        """Test that all 6 customers have mappings."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        customers = ["customer_a", "customer_b", "customer_c", 
                    "customer_d", "customer_e", "customer_f"]
        
        for concept_id in ["contract_identifier", "contract_value"]:
            for customer_id in customers:
                mapping = kg.get_mapping(concept_id, customer_id)
                assert mapping is not None, f"Missing {customer_id} mapping for {concept_id}"
    
    def test_customer_d_days_remaining(self):
        """Test that Customer D uses days_remaining with transformation."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        mapping = kg.get_mapping("contract_expiration", "customer_d")
        assert mapping is not None
        assert mapping.semantic_type == SemanticType.DAYS_REMAINING
        assert mapping.transformation is not None
    
    def test_customer_f_annual_value(self):
        """Test that Customer F uses annual values with transformation."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        mapping = kg.get_mapping("contract_value", "customer_f")
        assert mapping is not None
        assert mapping.semantic_type == SemanticType.ANNUAL_RECURRING_REVENUE
        assert mapping.transformation is not None
    
    def test_customer_b_join_requirements(self):
        """Test that Customer B has join requirements."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        mapping = kg.get_mapping("contract_expiration", "customer_b")
        assert mapping is not None
        assert len(mapping.join_requirements) > 0
    
    def test_transformations_exist(self):
        """Test that transformation rules exist."""
        kg = SchemaKnowledgeGraph()
        kg.load()
        
        # Days to date
        transform = kg.get_transformation("days_remaining", "date")
        assert transform is not None
        
        # Annual to lifetime
        transform = kg.get_transformation("annual", "lifetime")
        assert transform is not None
