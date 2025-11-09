# Schema Translator

An intelligent contract schema translation system that enables querying across multiple enterprise customers with heterogeneous database schemas using LLM-powered semantic understanding.

## Prerequisites

- Python 3.12+
- Anthropic API key

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/sanzgiri/schema_translator.git
cd schema_translator
```

### 2. Create Virtual Environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 5. Generate Mock Data

```bash
python -m schema_translator.mock_data
```

### 6. Run Tests

```bash
pytest tests/
```

### 7. Start the Application

**Local Development:**
```bash
chainlit run app.py
```

**Railway Deployment:**
The app is configured for Railway deployment with automatic database initialization. Push to the `main` branch to trigger deployment.

## Project Structure

```
schema_translator/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── databases/                      # SQLite databases
├── schema_translator/              # Main package
│   ├── config.py                   # Configuration management
│   ├── models.py                   # Pydantic data models
│   ├── mock_data.py                # Mock data generation
│   ├── knowledge_graph.py          # Schema knowledge graph
│   ├── query_compiler.py           # SQL generation
│   ├── database_executor.py        # Query execution
│   ├── result_harmonizer.py        # Result normalization
│   ├── orchestrator.py             # Main pipeline orchestrator
│   ├── agents/                     # LLM agents
│   └── learning/                   # Learning and feedback
├── tests/                          # Test suite
└── app.py                          # Chainlit application
```

## Tech Stack

- **Language:** Python 3.12+
- **LLM:** Anthropic Claude (claude-sonnet-4-20250514)
- **Database:** SQLite
- **UI Framework:** Chainlit
- **Data Validation:** Pydantic
- **Graph:** NetworkX
- **Testing:** pytest
- **Environment:** python-dotenv
- **Deployment:** Railway

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Code Formatting

```bash
black schema_translator/ tests/
```

### Linting

```bash
ruff check schema_translator/ tests/
```

### Type Checking

```bash
mypy schema_translator/
```

## License

See LICENSE file for details.
