#!/usr/bin/env python3
"""Fix test_agents.py field names to match actual models."""

import re

# Read the file
with open("tests/test_agents.py", "r") as f:
    content = f.read()

# Fix SchemaColumn: column_name -> name (keep SchemaColumn only)
content = re.sub(r'(SchemaColumn\([^)]*?)column_name=', r'\1name=', content)

# Fix SchemaTable: table_name -> name (keep SchemaTable only)  
content = re.sub(r'(SchemaTable\([^)]*?)table_name=', r'\1name=', content)

# Add customer_name to CustomerSchema
content = re.sub(
    r'(customer_id="test_customer",)\n',
    r'\1\n            customer_name="Test Customer",\n',
    content
)
content = re.sub(
    r'(customer_id="test_new_customer",)\n',
    r'\1\n            customer_name="Test New Customer",\n',
    content
)

# Fix ConceptMapping - remove concept field, keep table_name and column_name, add data_type
# Find ConceptMapping blocks and fix them
concept_mapping_pattern = r'ConceptMapping\(\s*concept="([^"]+)",\s*customer_id="([^"]+)",\s*table_name="([^"]+)",\s*column_name="([^"]+)",\s*semantic_type=([^,\)]+)[,\s]*\)'

def fix_concept_mapping(match):
    concept = match.group(1)
    customer_id = match.group(2)
    table_name = match.group(3)
    column_name = match.group(4)
    semantic_type = match.group(5)
    
    # Determine data_type based on context
    if "total_amount" in column_name or "end_date" in column_name:
        if "date" in column_name:
            data_type = '"DATE"'
        else:
            data_type = '"DECIMAL(15,2)"'
    else:
        data_type = '"TEXT"'
    
    return f'''ConceptMapping(
                customer_id="{customer_id}",
                table_name="{table_name}",
                column_name="{column_name}",
                data_type={data_type},
                semantic_type={semantic_type},
            )'''

# Apply the fix
content = re.sub(concept_mapping_pattern, fix_concept_mapping, content, flags=re.MULTILINE | re.DOTALL)

# Write back
with open("tests/test_agents.py", "w") as f:
    f.write(content)

print("Fixed test_agents.py")
