# Schema Translator UI Guide

## Overview

The Schema Translator UI provides an intuitive chat interface for querying customer databases using natural language. The system automatically translates your queries into SQL and harmonizes results across different customer schemas.

## Getting Started

### Starting the Application

```bash
# Activate your virtual environment
source .venv/bin/activate

# Start the Chainlit server
chainlit run app.py --port 8000
```

The application will be available at http://localhost:8000

### First Steps

1. Open the application in your browser
2. Read the welcome message to understand available commands
3. Try a simple query like "Show me all contracts"

## Query Examples

### Basic Queries

**List all records:**
```
Show me all contracts
Find all contracts
List contracts
```

**Filter by status:**
```
Show active contracts
Find pending contracts
List completed contracts
```

**Filter by value:**
```
Find contracts with value over 10000
Show contracts worth more than 50000
List high-value contracts (over 100000)
```

**Filter by dates:**
```
Show contracts expiring soon
Find contracts expiring in next 30 days
List recent contracts
```

### Advanced Queries

**Aggregation queries:**
```
Count contracts by status
Sum contract values by customer
Average contract value
```

**Sorting:**
```
Show contracts ordered by value
List contracts sorted by expiration date
```

**Combining filters:**
```
Find active contracts with value over 10000
Show pending contracts expiring in next 60 days
List high-value active contracts
```

## Available Commands

### `/help`
Display help information with examples and available commands.

**Usage:**
```
/help
```

### `/customers`
List all available customers with their database information.

**Usage:**
```
/customers
```

**Output:**
- Customer IDs
- Number of rows per customer
- Number of concepts mapped

### `/select <customer_id>`
Query specific customer(s) instead of all customers.

**Usage:**
```
# Query single customer
/select customer_a

# Query multiple customers (comma-separated)
/select customer_a, customer_c, customer_e

# Query all customers (default)
/select all
```

### `/debug on|off`
Toggle debug mode to see SQL queries and semantic plans.

**Usage:**
```
# Enable debug mode
/debug on

# Disable debug mode
/debug off

# Check current status
/debug
```

**Debug Information Includes:**
- Semantic query plan (intent, projections, filters)
- Generated SQL queries for each customer
- Execution details

### `/stats`
Display query execution statistics and knowledge graph information.

**Usage:**
```
/stats
```

**Shows:**
- Total queries executed
- Success rate
- Average execution time
- Knowledge graph statistics (concepts, customers, mappings)

### `/explain <query>`
Explain how a query will be processed without executing it.

**Usage:**
```
/explain Show me all active contracts
```

**Output:**
- Human-readable explanation
- Sample SQL query

## Understanding Results

### Result Table
Query results are displayed as markdown tables with:
- Column headers (concept names)
- Harmonized values (normalized across customers)
- Row count (limited to first 50 rows)

### Execution Statistics
Each result includes:
- **Success Rate:** Percentage of customers successfully queried
- **Total Rows:** Number of results returned
- **Customers Queried:** List of customers included
- **Customers Succeeded:** List of customers that returned results
- **Execution Time:** Query execution time in milliseconds

### Action Buttons
After each query, you can:
- 📖 **Explain Query:** See how the query was processed
- 👍 **Good Result:** Provide positive feedback
- 👎 **Incorrect Result:** Report incorrect results
- 🔍 **Show Debug Info:** View SQL and semantic plan (if debug mode off)

## Features

### Multi-Customer Queries
By default, queries run across all available customers. Results are automatically harmonized and combined.

**Benefits:**
- Single query syntax works for all customers
- Automatic schema translation
- Normalized results

### Query Modes

**Mock Mode:**
- Uses predefined query patterns
- No LLM API calls required
- Fast response times
- Good for testing

**LLM Mode:**
- Uses Claude Sonnet 4 for query understanding
- Better natural language understanding
- Requires ANTHROPIC_API_KEY
- More flexible query handling

### Debug Mode
When enabled, debug mode shows:
1. **Semantic Plan:**
   - Intent (e.g., find_contracts)
   - Projections (fields to return)
   - Filters (conditions)
   - Aggregations (if any)

2. **Generated SQL:**
   - Customer-specific SQL queries
   - Shows schema differences

3. **Execution Details:**
   - Per-customer execution status
   - Error messages if any

## Tips and Best Practices

### Writing Effective Queries

**Be specific:**
✅ "Show active contracts with value over 10000"
❌ "Show stuff"

**Use natural language:**
✅ "Find contracts expiring in next 30 days"
❌ "SELECT * WHERE exp_date < NOW() + 30"

**Use common terms:**
- "active", "pending", "completed" for status
- "high value", "over X", "more than X" for amounts
- "expiring soon", "recent", "next X days" for dates

### Performance

**For faster queries:**
- Query specific customers instead of all
- Use filters to limit results
- Avoid complex aggregations when not needed

**For comprehensive analysis:**
- Query all customers
- Use aggregation functions
- Enable debug mode to see execution details

### Troubleshooting

**Query returns no results:**
- Check if selected customers have data
- Try broader filters
- Use `/customers` to see available data

**Query fails with error:**
- Check query syntax (at least 3 characters)
- Enable debug mode to see details
- Try simpler query first
- Check `/stats` for system status

**Slow queries:**
- Reduce number of customers
- Add more specific filters
- Check debug info for complex SQL

## Configuration

### Environment Variables
The application uses `.env` file for configuration:

```env
# Optional: Anthropic API key for LLM mode
ANTHROPIC_API_KEY=your_api_key_here

# Optional: Model selection
LLM_MODEL=claude-sonnet-4-20250514

# Optional: Database directory
DATABASE_DIR=./data/databases
```

### Chainlit Configuration
Edit `.chainlit/config.toml` to customize:
- UI theme
- Session timeout
- File upload settings
- Feature flags

## Keyboard Shortcuts

While in the chat interface:
- **Enter:** Send message
- **Shift + Enter:** New line in message
- **Ctrl/Cmd + K:** Clear chat
- **Ctrl/Cmd + L:** Toggle light/dark mode

## Advanced Features

### Custom Customer Selection
You can maintain a session-specific customer selection:

```
# Set selection
/select customer_a, customer_b

# All queries will now use only these customers
Show me all contracts

# Reset to all customers
/select all
```

### Query History
The system tracks all queries in the session:
- Success/failure status
- Execution time
- Timestamp
- View with `/stats` command

### Feedback Loop
Use feedback buttons to improve results:
- 👍 Mark good results
- 👎 Report incorrect results
- Feedback stored for future improvements (Phase 8)

## Example Workflows

### Exploring Available Data
```
1. /customers                    # See what's available
2. /select customer_a            # Focus on one customer
3. Show me all contracts         # See sample data
4. /debug on                     # Enable debug mode
5. Find active contracts         # See how queries work
6. /select all                   # Return to all customers
```

### Complex Analysis
```
1. /debug on                           # Enable debugging
2. Count contracts by status           # Get overview
3. Find high-value contracts           # Identify valuable ones
4. Show active contracts over 50000    # Combine filters
5. /stats                              # Check performance
```

### Troubleshooting Issues
```
1. /debug on                     # Enable detailed output
2. <your failing query>          # Run the query
3. Check semantic plan           # Verify interpretation
4. Check SQL queries             # Verify generated SQL
5. Simplify query and retry      # Try simpler version
```

## Support and Documentation

### Getting Help
- Use `/help` in chat for quick reference
- Check this guide for detailed information
- Enable debug mode to diagnose issues
- Check `/stats` for system health

### Known Limitations
- Maximum 50 rows displayed per query (full results processed)
- Query text must be at least 3 characters
- Mock mode has limited query patterns
- Some complex queries require LLM mode

### Future Enhancements (Phase 8)
- Learning from feedback
- Schema drift detection
- Query optimization suggestions
- Improved error recovery
- Enhanced analytics

## Quick Reference Card

| Command | Purpose | Example |
|---------|---------|---------|
| `/help` | Show help | `/help` |
| `/customers` | List customers | `/customers` |
| `/select` | Choose customers | `/select customer_a, customer_b` |
| `/debug` | Toggle debug | `/debug on` |
| `/stats` | Show statistics | `/stats` |
| `/explain` | Explain query | `/explain Find contracts` |

| Query Type | Example |
|------------|---------|
| List all | `Show me all contracts` |
| Filter status | `Find active contracts` |
| Filter value | `Show contracts over 10000` |
| Filter date | `Find contracts expiring soon` |
| Aggregate | `Count contracts by status` |
| Combined | `Show active contracts over 10000` |

---

**Version:** Phase 7 - UI Implementation
**Last Updated:** November 7, 2025
