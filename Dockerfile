FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose Chainlit default port
EXPOSE 8000

# Set environment variables for WebSocket support
ENV CHAINLIT_HOST=0.0.0.0

# Run setup then start app
CMD python setup_env.py && chainlit run app.py --host 0.0.0.0 --headless
