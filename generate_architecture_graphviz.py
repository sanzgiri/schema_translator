"""
Generate Schema Translator Architecture using Graphviz.

Installation:
    pip install graphviz

Usage:
    python generate_architecture_graphviz.py

Output:
    schema_translator_architecture_graphviz.png
"""

from graphviz import Digraph

def create_architecture_diagram():
    """Create the Schema Translator architecture diagram using Graphviz."""
    
    dot = Digraph(comment='Schema Translator Architecture')
    dot.attr(rankdir='TB', bgcolor='white', pad='0.5', ranksep='0.3', nodesep='0.5')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightgray', 
             fontname='Helvetica', fontsize='12', fontcolor='black')
    dot.attr('edge', fontname='Helvetica', fontsize='10', color='black', fontcolor='black')
    
    # User Interface Layer - Lightest gray
    dot.node('ui', 'Chainlit LLM Chat UI', 
             fillcolor='#F5F5F5', shape='box', style='rounded,filled')
    
    # Orchestration Layer - Light gray
    dot.node('orchestrator', 'Chat Orchestrator', 
             fillcolor='#E0E0E0', style='rounded,filled')
    
    # Core Components - Medium gray shades
    with dot.subgraph(name='cluster_0') as c:
        c.attr(label='Core Components', style='dashed', color='gray')
        c.node('query_agent', 'Query Understanding\nAgent\n(Claude Sonnet 4.5)', 
               fillcolor='#D0D0D0')
        c.node('kg', 'Knowledge Graph\n(NetworkX)', fillcolor='#C0C0C0')
        c.node('harmonizer', 'Result Harmonizer\n(Pydantic)', fillcolor='#B0B0B0')
    
    # SQL Generation - Medium-dark gray
    dot.node('compiler', 'Query Compiler\n(SQL Generator)', fillcolor='#A0A0A0')
    
    # Database Execution - Darker gray
    dot.node('executor', 'Database Executor\n(SQLite)', fillcolor='#909090')
    
    # Customer Databases (same rank for parallel layout) - Light gray
    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('db_a', 'Customer A\nDB', fillcolor='#D5D5D5', shape='cylinder')
        s.node('db_b', 'Customer B\nDB', fillcolor='#D5D5D5', shape='cylinder')
        s.node('db_c', 'Customer C\nDB', fillcolor='#D5D5D5', shape='cylinder')
        s.node('db_d', 'Customer D\nDB', fillcolor='#D5D5D5', shape='cylinder')
        s.node('db_e', 'Customer E\nDB', fillcolor='#D5D5D5', shape='cylinder')
        s.node('db_f', 'Customer F\nDB', fillcolor='#D5D5D5', shape='cylinder')
    
    # Forward Flow (Query Path) - Black arrows
    dot.edge('ui', 'orchestrator', label='1. User Query')
    dot.edge('orchestrator', 'query_agent', label='2. Parse')
    dot.edge('orchestrator', 'kg', label='3. Get Mappings')
    dot.edge('query_agent', 'compiler', label='4. Semantic Plan')
    dot.edge('kg', 'compiler', label='5. Schema Mappings')
    dot.edge('compiler', 'executor', label='6. Generate SQL')
    
    # Parallel execution to all databases
    dot.edge('executor', 'db_a')
    dot.edge('executor', 'db_b')
    dot.edge('executor', 'db_c')
    dot.edge('executor', 'db_d')
    dot.edge('executor', 'db_e')
    dot.edge('executor', 'db_f')
    
    # Backward Flow (Results Path) - Black arrows
    dot.edge('executor', 'harmonizer', label='7. Raw Results')
    dot.edge('harmonizer', 'orchestrator', label='8. Harmonized Results')
    dot.edge('orchestrator', 'ui', label='9. Display')
    
    return dot


def main():
    """Generate and save the diagram."""
    dot = create_architecture_diagram()
    
    # Save as PNG
    output_file = 'schema_translator_architecture_graphviz'
    dot.render(output_file, format='png', cleanup=True)
    print(f"✓ Architecture diagram generated: {output_file}.png")
    
    # Also save the DOT source
    with open(f'{output_file}.dot', 'w') as f:
        f.write(dot.source)
    print(f"✓ DOT source saved: {output_file}.dot")


if __name__ == "__main__":
    main()
