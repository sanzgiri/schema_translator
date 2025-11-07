"""Knowledge graph for semantic schema mappings."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import networkx as nx

from schema_translator.config import get_config
from schema_translator.models import ConceptMapping, SemanticConcept, SemanticType


class SchemaKnowledgeGraph:
    """Manages semantic relationships between customer schemas and concepts."""
    
    def __init__(self):
        """Initialize the knowledge graph."""
        self.graph = nx.DiGraph()
        self.concepts: Dict[str, SemanticConcept] = {}
        self.transformations: Dict[str, Dict[str, str]] = {}
        self.config = get_config()
    
    def add_concept(
        self,
        concept_id: str,
        concept_name: str,
        description: str,
        aliases: Optional[List[str]] = None
    ) -> SemanticConcept:
        """Add a semantic concept to the knowledge graph.
        
        Args:
            concept_id: Unique identifier for the concept
            concept_name: Human-readable name
            description: Description of what this concept represents
            aliases: Alternative names for this concept
            
        Returns:
            The created SemanticConcept
        """
        concept = SemanticConcept(
            concept_id=concept_id,
            concept_name=concept_name,
            description=description,
            aliases=aliases or [],
            customer_mappings={}
        )
        
        self.concepts[concept_id] = concept
        self.graph.add_node(concept_id, type="concept", data=concept)
        
        return concept
    
    def add_customer_mapping(
        self,
        concept_id: str,
        customer_id: str,
        table_name: str,
        column_name: str,
        data_type: str,
        semantic_type: SemanticType,
        transformation: Optional[str] = None,
        join_requirements: Optional[List[str]] = None
    ) -> None:
        """Add a customer-specific mapping for a concept.
        
        Args:
            concept_id: The concept being mapped
            customer_id: Customer identifier
            table_name: Table containing this concept
            column_name: Column representing this concept
            data_type: SQL data type
            semantic_type: Semantic interpretation
            transformation: SQL transformation needed (optional)
            join_requirements: Additional tables for JOIN (optional)
        """
        if concept_id not in self.concepts:
            raise ValueError(f"Concept {concept_id} not found. Add it first with add_concept()")
        
        mapping = ConceptMapping(
            customer_id=customer_id,
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
            semantic_type=semantic_type,
            transformation=transformation,
            join_requirements=join_requirements or []
        )
        
        # Add to concept
        self.concepts[concept_id].customer_mappings[customer_id] = mapping
        
        # Add to graph
        customer_node = f"{customer_id}:{concept_id}"
        self.graph.add_node(customer_node, type="mapping", data=mapping)
        self.graph.add_edge(concept_id, customer_node, relation="has_mapping")
    
    def add_transformation(
        self,
        from_type: str,
        to_type: str,
        transformation_sql: str
    ) -> None:
        """Add a transformation rule between semantic types.
        
        Args:
            from_type: Source semantic type
            to_type: Target semantic type
            transformation_sql: SQL transformation template
        """
        if from_type not in self.transformations:
            self.transformations[from_type] = {}
        
        self.transformations[from_type][to_type] = transformation_sql
    
    def get_concept(self, concept_id: str) -> Optional[SemanticConcept]:
        """Get a concept by ID.
        
        Args:
            concept_id: Concept identifier
            
        Returns:
            SemanticConcept if found, None otherwise
        """
        return self.concepts.get(concept_id)
    
    def get_mapping(
        self,
        concept_id: str,
        customer_id: str
    ) -> Optional[ConceptMapping]:
        """Get a customer-specific mapping for a concept.
        
        Args:
            concept_id: Concept identifier
            customer_id: Customer identifier
            
        Returns:
            ConceptMapping if found, None otherwise
        """
        concept = self.concepts.get(concept_id)
        if concept:
            return concept.get_mapping(customer_id)
        return None
    
    def get_transformation(
        self,
        from_type: str,
        to_type: str
    ) -> Optional[str]:
        """Get transformation SQL from one type to another.
        
        Args:
            from_type: Source semantic type
            to_type: Target semantic type
            
        Returns:
            SQL transformation template if found, None otherwise
        """
        if from_type in self.transformations:
            return self.transformations[from_type].get(to_type)
        return None
    
    def find_concept_by_alias(self, alias: str) -> Optional[SemanticConcept]:
        """Find a concept by its name or alias.
        
        Args:
            alias: Concept name or alias to search for
            
        Returns:
            SemanticConcept if found, None otherwise
        """
        alias_lower = alias.lower()
        
        for concept in self.concepts.values():
            if concept.concept_id.lower() == alias_lower:
                return concept
            if concept.concept_name.lower() == alias_lower:
                return concept
            if any(a.lower() == alias_lower for a in concept.aliases):
                return concept
        
        return None
    
    def get_all_concepts(self) -> List[SemanticConcept]:
        """Get all concepts in the knowledge graph.
        
        Returns:
            List of all SemanticConcepts
        """
        return list(self.concepts.values())
    
    def get_customers_for_concept(self, concept_id: str) -> List[str]:
        """Get all customers that have a mapping for this concept.
        
        Args:
            concept_id: Concept identifier
            
        Returns:
            List of customer IDs
        """
        concept = self.concepts.get(concept_id)
        if concept:
            return list(concept.customer_mappings.keys())
        return []
    
    def save(self, path: Optional[Path] = None) -> None:
        """Save knowledge graph to JSON file.
        
        Args:
            path: Optional custom path, uses config default if not provided
        """
        if path is None:
            path = self.config.knowledge_graph_path
        
        data = {
            "concepts": {
                concept_id: concept.model_dump()
                for concept_id, concept in self.concepts.items()
            },
            "transformations": self.transformations
        }
        
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def load(self, path: Optional[Path] = None) -> None:
        """Load knowledge graph from JSON file.
        
        Args:
            path: Optional custom path, uses config default if not provided
        """
        if path is None:
            path = self.config.knowledge_graph_path
        
        if not path.exists():
            raise FileNotFoundError(f"Knowledge graph file not found: {path}")
        
        with open(path, "r") as f:
            data = json.load(f)
        
        # Clear existing data
        self.graph.clear()
        self.concepts.clear()
        self.transformations.clear()
        
        # Load concepts
        for concept_id, concept_data in data.get("concepts", {}).items():
            concept = SemanticConcept(**concept_data)
            self.concepts[concept_id] = concept
            self.graph.add_node(concept_id, type="concept", data=concept)
            
            # Add mapping nodes
            for customer_id, mapping_data in concept.customer_mappings.items():
                customer_node = f"{customer_id}:{concept_id}"
                self.graph.add_node(customer_node, type="mapping", data=mapping_data)
                self.graph.add_edge(concept_id, customer_node, relation="has_mapping")
        
        # Load transformations
        self.transformations = data.get("transformations", {})
    
    def validate(self) -> Dict[str, Any]:
        """Validate the knowledge graph for completeness.
        
        Returns:
            Dictionary with validation results
        """
        issues = []
        warnings = []
        
        # Check that all concepts have at least one mapping
        for concept_id, concept in self.concepts.items():
            if not concept.customer_mappings:
                warnings.append(f"Concept '{concept_id}' has no customer mappings")
        
        # Check that all customers have mappings for core concepts
        core_concepts = [
            "contract_identifier",
            "contract_expiration",
            "contract_value"
        ]
        
        all_customers = set()
        for concept in self.concepts.values():
            all_customers.update(concept.customer_mappings.keys())
        
        for customer_id in all_customers:
            for core_concept in core_concepts:
                if core_concept in self.concepts:
                    if customer_id not in self.concepts[core_concept].customer_mappings:
                        issues.append(
                            f"Customer '{customer_id}' missing core concept '{core_concept}'"
                        )
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "concepts_count": len(self.concepts),
            "customers_count": len(all_customers),
            "transformations_count": sum(len(v) for v in self.transformations.values())
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the knowledge graph.
        
        Returns:
            Dictionary with graph statistics
        """
        all_customers = set()
        for concept in self.concepts.values():
            all_customers.update(concept.customer_mappings.keys())
        
        return {
            "total_concepts": len(self.concepts),
            "total_customers": len(all_customers),
            "total_mappings": sum(len(c.customer_mappings) for c in self.concepts.values()),
            "total_transformations": sum(len(v) for v in self.transformations.values()),
            "graph_nodes": self.graph.number_of_nodes(),
            "graph_edges": self.graph.number_of_edges()
        }
    
    def __repr__(self) -> str:
        """String representation of the knowledge graph."""
        stats = self.get_stats()
        return (
            f"SchemaKnowledgeGraph("
            f"concepts={stats['total_concepts']}, "
            f"customers={stats['total_customers']}, "
            f"mappings={stats['total_mappings']})"
        )
