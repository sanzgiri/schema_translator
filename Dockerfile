FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose Chainlit default port for HF Spaces
EXPOSE 7860

# Set environment variables for WebSocket support
ENV CHAINLIT_HOST=0.0.0.0
ENV CHAINLIT_PORT=7860

# Run setup then start app (setup runs at runtime when secrets are available)
CMD python setup_env.py && chainlit run app.py --host 0.0.0.0 --port 7860 --headless
