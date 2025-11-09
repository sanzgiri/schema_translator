#!/bin/bash

# Initialize environment (databases + knowledge graph) if needed
python setup_env.py

# Export port for Chainlit
export CHAINLIT_PORT=${PORT:-8000}

# Start Chainlit server
chainlit run app.py --host 0.0.0.0 --headless
