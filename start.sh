#!/bin/bash
export CHAINLIT_PORT=${PORT:-8000}
chainlit run app.py --host 0.0.0.0 --headless
