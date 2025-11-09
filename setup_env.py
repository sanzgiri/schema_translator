#!/usr/bin/env python3
"""Setup environment for Hugging Face Spaces deployment."""

from pathlib import Path
import sys

def setup():
    """Initialize databases and knowledge graph if they don't exist."""
    
    # Check if already initialized
    if Path("databases").exists() and Path("knowledge_graph.json").exists():
        print("✓ Environment already initialized")
        return
    
    print("🚀 Initializing environment for first run...")
    
    # Generate databases
    print("\n1. Generating customer databases...")
    try:
        from schema_translator.mock_data import main as generate_databases
        generate_databases()
    except Exception as e:
        print(f"❌ Error generating databases: {e}")
        sys.exit(1)
    
    # Generate knowledge graph
    print("\n2. Initializing knowledge graph...")
    try:
        from initialize_kg import main as initialize_kg
        initialize_kg()
    except Exception as e:
        print(f"❌ Error initializing knowledge graph: {e}")
        sys.exit(1)
    
    print("\n✓ Environment setup complete!")

if __name__ == "__main__":
    setup()
