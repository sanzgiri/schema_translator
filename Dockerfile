FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Setup environment (generate databases & knowledge graph)
RUN python setup_env.py

# Expose Chainlit default port for HF Spaces
EXPOSE 7860

# Run the Chainlit app
CMD ["chainlit", "run", "app.py", "--host", "0.0.0.0", "--port", "7860"]
