---
title: Schema Translator
emoji: 🔄
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: 5.0.0
app_file: app_gradio.py
pinned: false
license: mit
tags:
  - llm
  - database
  - schema-translation
  - natural-language-query
python_version: "3.12"
---

# Schema Translator

An intelligent contract schema translation system that enables querying across multiple enterprise customers with heterogeneous database schemas using LLM-powered semantic understanding.

## Prerequisites

- Python 3.10+
- UV package manager
- Anthropic API key

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repo-url>
cd schema_translator_v2
```

### 2. Create Virtual Environment with UV

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
uv pip install -r requirements.txt
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

```bash
chainlit run app.py
```

## Project Structure

```
schema_translator_v2/
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

- **Language:** Python 3.10+
- **LLM:** Anthropic Claude (claude-sonnet-4-20250514)
- **Database:** SQLite
- **UI Framework:** Chainlit
- **Data Validation:** Pydantic
- **Graph:** NetworkX
- **Testing:** pytest
- **Environment:** python-dotenv

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
