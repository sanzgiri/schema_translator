#!/bin/bash

# Schema Translator Setup Verification Script

echo "🔍 Verifying Schema Translator Setup..."
echo ""

# Check if virtual environment exists
if [ -d ".venv" ]; then
    echo "✓ Virtual environment exists"
else
    echo "✗ Virtual environment not found"
    echo "  Run: uv venv"
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Check if .env exists
if [ -f ".env" ]; then
    echo "✓ .env file exists"
else
    echo "⚠ .env file not found"
    echo "  Run: cp .env.example .env"
    echo "  Then edit .env and add your ANTHROPIC_API_KEY"
fi

# Check key packages
echo ""
echo "📦 Checking installed packages..."
python -c "
import anthropic
import pydantic
import chainlit
import networkx
import pytest
print('✓ anthropic:', anthropic.__version__)
print('✓ pydantic:', pydantic.__version__)
print('✓ chainlit:', chainlit.__version__)
print('✓ networkx:', networkx.__version__)
print('✓ pytest:', pytest.__version__)
"

echo ""
echo "✅ Environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Copy .env.example to .env: cp .env.example .env"
echo "2. Add your ANTHROPIC_API_KEY to .env"
echo "3. Create project structure: mkdir -p schema_translator/agents schema_translator/learning tests databases"
echo "4. Start building components per requirements document"
echo ""
echo "Activate environment: source .venv/bin/activate"
