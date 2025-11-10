"""
Chainlit UI for Schema Translator

This module provides a chat interface for querying customer databases
using natural language queries that are automatically translated to SQL.
"""

import chainlit as cl
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from schema_translator.orchestrator import ChatOrchestrator
from schema_translator.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global orchestrator instance
orchestrator: Optional[ChatOrchestrator] = None


def format_result_table(result, limit: int = 10) -> str:
    """Format harmonized result as a markdown table.
    
    Args:
        result: HarmonizedResult object
        limit: Maximum number of rows to display (default 10)
        
    Returns:
        Markdown formatted table
    """
    if not result.results:
        return "*No results found*"
    
    # Get column names from first row
    if not result.results:
        return "*No data*"
    
    first_row = result.results[0]
    all_columns = list(first_row.data.keys())
    
    # Add source database to the data (from customer_id in HarmonizedRow)
    # Convert customer_a -> A, customer_b -> B, etc. for display
    for row in result.results:
        if 'source_db' not in row.data:
            # Extract letter from customer_id (customer_a -> A)
            customer_letter = row.customer_id.replace('customer_', '').upper()
            row.data['source_db'] = customer_letter
    
    # Add source_db to all_columns if not already there
    if 'source_db' not in all_columns:
        all_columns.insert(0, 'source_db')
    
    # Filter to only columns that have non-null values in at least one row
    columns_with_values = set()
    for col in all_columns:
        if any(row.data.get(col) is not None for row in result.results):
            columns_with_values.add(col)
    
    # Check if multiple customers are being queried
    multiple_customers = len(result.customers_queried) > 1
    
    # Always include source_db if querying multiple customers
    if multiple_customers and 'source_db' not in columns_with_values:
        columns_with_values.add('source_db')
    
    # Check if this is an aggregation/count query (no contract_identifier or all null)
    is_aggregation = 'contract_identifier' not in columns_with_values
    
    if is_aggregation:
        # For aggregations, show source_db first, then all other non-null columns
        columns = []
        if 'source_db' in columns_with_values:
            columns.append('source_db')
        for col in columns_with_values:
            if col != 'source_db':
                columns.append(col)
        
        # Nice names for aggregation fields
        nice_names = {
            'source_db': 'Customer',
            'count_contract_identifier': 'Count',
            'sum_contract_value': 'Total Value',
            'avg_contract_value': 'Avg Value',
            'max_contract_value': 'Max Value',
            'min_contract_value': 'Min Value',
            'count': 'Count',
            'sum': 'Sum',
            'average': 'Average',
            'max': 'Max',
            'min': 'Min'
        }
    else:
        # For regular queries, use preferred field order (only for columns with values)
        field_order = [
            'source_db',  # Show which customer (A, B, C, etc.)
            'contract_identifier',  # Contract ID
            'contract_value',
            'contract_status',
            'contract_expiration',
            'contract_start'
        ]
        
        nice_names = {
            'source_db': 'Customer',  # Which database (A, B, C, D, E, F)
            'contract_identifier': 'Contract ID',
            'contract_value': 'Value',
            'contract_status': 'Status',
            'contract_expiration': 'Expiration',
            'contract_start': 'Start Date'
        }
        
        # Order columns: preferred order first (only include if they have values)
        columns = []
        for field in field_order:
            if field in columns_with_values:
                columns.append(field)
        
        # Add any remaining columns not in preferred order (that have values)
        for col in columns_with_values:
            if col not in columns:
                columns.append(col)
    
    # Build markdown table with better formatting
    lines = []
    
    # Header with nicer column names
    display_cols = []
    for col in columns:
        # Get nice name or convert snake_case to Title Case
        if col in nice_names:
            display_cols.append(nice_names[col])
        else:
            # Convert snake_case to Title Case
            display_cols.append(col.replace('_', ' ').title())
    
    header = "| " + " | ".join(display_cols) + " |"
    separator = "|" + "|".join(["---" for _ in columns]) + "|"
    lines.append(header)
    lines.append(separator)
    
    # Rows (limit to specified number or total results, whichever is smaller)
    max_rows = min(limit, len(result.results))
    for row in result.results[:max_rows]:
        values = []
        for col in columns:
            val = row.data.get(col, "")
            # Format values for better display
            if val is None or val == "None":
                val = "—"
            elif isinstance(val, (int, float)):
                # Format numbers based on column type
                if 'value' in col.lower() or 'sum' in col.lower():
                    # Format currency values
                    val = f"${val:,.0f}"
                elif 'avg' in col.lower() or 'average' in col.lower():
                    # Format averages with 2 decimals
                    val = f"${val:,.2f}" if 'value' in col.lower() else f"{val:,.2f}"
                else:
                    # Format counts and other integers
                    val = f"{val:,}"
            else:
                val = str(val)
            values.append(val)
        line = "| " + " | ".join(values) + " |"
        lines.append(line)
    
    if len(result.results) > max_rows:
        lines.append(f"\n*Showing {max_rows} of {len(result.results)} rows. Use `/limit <number>` to adjust.*")
    
    return "\n".join(lines)


def format_statistics(stats: Dict[str, Any]) -> str:
    """Format statistics as markdown.
    
    Args:
        stats: Statistics dictionary
        
    Returns:
        Formatted statistics
    """
    lines = [
        "### 📊 Execution Statistics",
        f"- **Total Rows:** {stats['total_rows']}",
        f"- **Customers Queried:** {len(stats['customers_queried'])}",
        f"- **Execution Time:** {stats['execution_time_ms']:.2f}ms"
    ]
    
    if stats['customers_failed']:
        lines.append(f"- **Customers Failed:** {', '.join(stats['customers_failed'])}")
    
    return "\n".join(lines)


def format_debug_info(debug: Dict[str, Any]) -> str:
    """Format debug information as markdown.
    
    Args:
        debug: Debug information dictionary
        
    Returns:
        Formatted debug info
    """
    projections = debug['semantic_plan']['projections']
    proj_list = ', '.join(f'`{p}`' for p in projections) if projections else "*(none - SELECT ALL)*"
    
    lines = [
        "### 🔍 Debug Information",
        "",
        "**Semantic Plan:**",
        f"- Intent: `{debug['semantic_plan']['intent']}`",
        f"- Projections ({len(projections)}): {proj_list}",
    ]
    
    if debug['semantic_plan']['filters']:
        lines.append(f"- Filters: {len(debug['semantic_plan']['filters'])}")
        for f in debug['semantic_plan']['filters']:
            lines.append(f"  - `{f['concept']}` {f['operator']} `{f['value']}`")
    
    if debug['semantic_plan']['aggregations']:
        agg_strs = []
        for agg in debug['semantic_plan']['aggregations']:
            if agg.get('alias'):
                agg_strs.append(f"{agg['function']}({agg['concept']}) as {agg['alias']}")
            else:
                agg_strs.append(f"{agg['function']}({agg['concept']})")
        lines.append(f"- Aggregations: {', '.join(agg_strs)}")
    
    # Show actual columns returned
    if 'actual_columns' in debug:
        actual_cols = ', '.join(f'`{c}`' for c in debug['actual_columns'])
        lines.append(f"\n**Actual Columns Returned ({len(debug['actual_columns'])}):**")
        lines.append(actual_cols)
    
    # Show sample SQL for first customer
    lines.append("\n**Sample SQL (first customer):**")
    if debug['sql_queries']:
        first_customer = list(debug['sql_queries'].keys())[0]
        sql = debug['sql_queries'][first_customer]
        lines.append(f"```sql\n{sql}\n```")
    
    return "\n".join(lines)


@cl.on_chat_start
async def start():
    """Initialize the chat session."""
    global orchestrator
    
    # Initialize orchestrator
    try:
        config = Config()
        orchestrator = ChatOrchestrator(use_llm=bool(config.anthropic_api_key))
        
        # Store in session
        cl.user_session.set("orchestrator", orchestrator)
        cl.user_session.set("debug_mode", False)
        cl.user_session.set("selected_customers", [])  # Empty = all customers
        cl.user_session.set("result_limit", 10)  # Default rows to display
        
        mode = "LLM mode" if orchestrator.use_llm else "Mock mode"
        
        # Get available customers
        customers = orchestrator.list_available_customers()
        
        # Create settings panel - using list syntax for Chainlit 2.9.0
        settings = [
            cl.input_widget.Select(
                id="customer_selection",
                label="Query Customers",
                values=["All Customers"] + [f"Customer {c.split('_')[1].upper()}" for c in customers],
                initial_value="All Customers",
                description="Select which customer database(s) to query"
            ),
            cl.input_widget.Switch(
                id="debug_mode",
                label="Debug Mode",
                initial=False,
                description="Show SQL queries and semantic plans"
            ),
            cl.input_widget.Slider(
                id="result_limit",
                label="Max Results",
                initial=10,
                min=5,
                max=100,
                step=5,
                description="Maximum number of rows to display"
            )
        ]
        
        await cl.ChatSettings(settings).send()
        
        # Send welcome message
        welcome_msg = f"""# 🎯 Schema Translator

Query customer contracts databases using natural language. 

**Examples:**
- "Show me all contracts from customer A"
- "Find active contracts with value over $3M"
- "Contracts expiring in 30 days"
- "Count active contracts by customer"

**Settings:** Click the ⚙️ gear icon at the bottom of the chat input to select customers, enable debug mode, or adjust result limit.

**Commands:** `/help` • `/stats` • `/explain <query>`
"""
        
        await cl.Message(content=welcome_msg).send()
        
        logger.info(f"Chat started with {len(customers)} customers available")
        
    except Exception as e:
        logger.error(f"Failed to initialize orchestrator: {e}", exc_info=True)
        await cl.Message(
            content=f"❌ **Error:** Failed to initialize. Please check configuration.\n\n`{str(e)}`"
        ).send()


@cl.on_settings_update
async def on_settings_change(settings):
    """Handle settings changes from the sidebar."""
    orchestrator = cl.user_session.get("orchestrator")
    
    # Update debug mode
    debug_mode = settings.get("debug_mode", False)
    cl.user_session.set("debug_mode", debug_mode)
    
    # Update result limit
    result_limit = settings.get("result_limit", 10)
    cl.user_session.set("result_limit", result_limit)
    
    # Update customer selection
    customer_selection = settings.get("customer_selection", "All Customers")
    if customer_selection == "All Customers":
        cl.user_session.set("selected_customers", [])
        await cl.Message(content="✅ Now querying **all customers**.").send()
    else:
        # Extract customer ID from "Customer A" format
        customer_letter = customer_selection.split()[-1].lower()
        customer_id = f"customer_{customer_letter}"
        cl.user_session.set("selected_customers", [customer_id])
        await cl.Message(content=f"✅ Now querying **{customer_id}**.").send()


@cl.on_message
async def main(message: cl.Message):
    """Handle incoming chat messages."""
    global orchestrator
    
    orchestrator = cl.user_session.get("orchestrator")
    debug_mode = cl.user_session.get("debug_mode", False)
    selected_customers = cl.user_session.get("selected_customers", [])
    result_limit = cl.user_session.get("result_limit", 10)
    
    if not orchestrator:
        await cl.Message(content="❌ Orchestrator not initialized. Please refresh.").send()
        return
    
    query_text = message.content.strip()
    
    # Handle commands
    if query_text.startswith("/"):
        await handle_command(query_text, orchestrator, debug_mode)
        return
    
    # Validate query
    if not query_text or len(query_text) < 3:
        await cl.Message(content="⚠️ Please enter a valid query (at least 3 characters).").send()
        return
    
    # Show processing message
    processing_msg = cl.Message(content="🤔 Processing your query...")
    await processing_msg.send()
    
    try:
        # Execute query
        customer_ids = selected_customers if selected_customers else None
        
        response = orchestrator.process_query(
            query_text,
            customer_ids=customer_ids,
            debug=debug_mode
        )
        
        if response['success']:
            # Format successful response
            result = response['result']
            
            # Build response message
            content_parts = []
            
            # Show available fields
            if result.results:
                fields = list(result.results[0].data.keys())
                # Filter out None values to get actual fields returned
                actual_fields = [f for f in fields if any(
                    row.data.get(f) is not None for row in result.results
                )]
                
                field_names = {
                    'source_db': 'Customer',
                    'contract_identifier': 'Contract ID',
                    'contract_value': 'Value',
                    'contract_status': 'Status',
                    'contract_expiration': 'Expiration Date',
                    'contract_start': 'Start Date',
                    'count_contract_identifier': 'Count',
                    'sum_contract_value': 'Total Value',
                    'avg_contract_value': 'Avg Value',
                    'max_contract_value': 'Max Value',
                    'min_contract_value': 'Min Value'
                }
                
                field_display = []
                for f in actual_fields:
                    nice_name = field_names.get(f, f.replace('_', ' ').title())
                    field_display.append(f"`{nice_name}`")
                
                field_list = ', '.join(field_display)
                content_parts.append(f"**Showing {len(actual_fields)} fields:** {field_list}\n")
            
            # Results table
            content_parts.append("### ✅ Query Results")
            content_parts.append(format_result_table(result, limit=result_limit))
            
            # Statistics
            stats = {
                'total_rows': result.total_count,
                'customers_queried': result.customers_queried,
                'customers_failed': result.customers_failed,
                'execution_time_ms': response['execution_time_ms']
            }
            content_parts.append("\n" + format_statistics(stats))
            
            # Debug info if enabled
            if debug_mode and 'debug' in response:
                # Add actual columns returned
                actual_columns = list(result.results[0].data.keys()) if result.results else []
                debug_with_columns = response['debug'].copy()
                debug_with_columns['actual_columns'] = actual_columns
                content_parts.append("\n" + format_debug_info(debug_with_columns))
            
            # Remove processing message and send result
            await processing_msg.remove()
            await cl.Message(content="\n\n".join(content_parts)).send()
            
            # Add action buttons
            actions = [
                cl.Action(name="explain", payload={"action": "explain"}, label="📖 Explain Query"),
                cl.Action(name="feedback_good", payload={"action": "good"}, label="👍 Good Result"),
                cl.Action(name="feedback_bad", payload={"action": "incorrect"}, label="👎 Incorrect Result"),
            ]
            
            if not debug_mode:
                actions.insert(0, cl.Action(name="debug", payload={"action": "debug"}, label="🔍 Show Debug Info"))
            
            await cl.Message(content="", actions=actions).send()
            
        else:
            # Format error response
            error_msg = f"""### ❌ Query Failed

**Error:** {response['error']}

**Suggestions:**
- Make sure your query is clear and specific
- Try using simpler language
- Use `/help` to see example queries
- Enable debug mode with `/debug on` for more details
"""
            await processing_msg.remove()
            await cl.Message(content=error_msg).send()
    
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        await processing_msg.remove()
        await cl.Message(
            content=f"❌ **Unexpected Error:** {str(e)}\n\nPlease try again or contact support."
        ).send()


@cl.action_callback("explain")
async def on_explain(action: cl.Action):
    """Handle explain action."""
    orchestrator = cl.user_session.get("orchestrator")
    
    # Get the query from the previous message
    # For now, send a message asking to use /explain command
    await cl.Message(
        content="To explain a query, type: `/explain <your query>`"
    ).send()


@cl.action_callback("feedback_good")
async def on_feedback_good(action: cl.Action):
    """Handle good feedback."""
    await cl.Message(content="✅ Thank you for the feedback!").send()


@cl.action_callback("feedback_bad")
async def on_feedback_bad(action: cl.Action):
    """Handle bad feedback."""
    await cl.Message(
        content="📝 Thank you for the feedback! We'll work on improving this. Please provide more details about what was incorrect."
    ).send()


@cl.action_callback("debug")
async def on_debug(action: cl.Action):
    """Handle debug action."""
    await cl.Message(
        content="🔍 Debug mode enabled for this session. Use `/debug on` to enable for all queries."
    ).send()


async def handle_command(command: str, orchestrator: ChatOrchestrator, debug_mode: bool):
    """Handle special commands.
    
    Args:
        command: Command string
        orchestrator: ChatOrchestrator instance
        debug_mode: Current debug mode state
    """
    parts = command.split(maxsplit=1)
    cmd = parts[0].lower()
    
    if cmd == "/help":
        help_msg = """### 📚 Help

**Example Queries:**
- "Show me all contracts"
- "Find active contracts"
- "Show contracts from customer A" or "from customer_a"
- "Query customer B and customer C databases"
- "Count contracts by status"
- "List contracts with value over 10000"
- "Show active contracts expiring in 90 days"
- "Contracts ending this year"

**Settings (⚙️ gear icon at bottom of chat input):**
- **Query Customers** - Select which customer database to query (or all)
- **Debug Mode** - Toggle to see SQL queries and semantic plans
- **Max Results** - Adjust how many rows to display (5-100)

**Important Notes:**
- You can query specific customers using the settings OR by saying "customer A", "from customer B", etc. in your query
- **Customer** column in results shows which database (A, B, C, etc.) each contract came from
- Settings changes apply to all subsequent queries

**Commands:**
- `/customers` - List available databases with details
- `/stats` - Show query statistics and knowledge graph info
- `/explain <query>` - Explain how a query will be processed
- `/help` - Show this help message

**Tips:**
- Use natural language - I'll understand it!
- Be specific about what you want to see
- Use filters like "active", "over $1M", "expiring in 30 days", "this year"
- Enable debug mode to see how queries are translated to SQL
"""
        await cl.Message(content=help_msg).send()
    
    elif cmd == "/customers":
        customers = orchestrator.list_available_customers()
        
        # Get info for each customer
        customer_info = []
        for customer_id in customers:
            info = orchestrator.get_customer_info(customer_id)
            if info['available']:
                customer_info.append(
                    f"- **{customer_id}**: {info['total_rows']} rows, "
                    f"{len(info['concepts'])} concepts mapped"
                )
        
        content = f"### 👥 Available Customers ({len(customers)})\n\n" + "\n".join(customer_info)
        await cl.Message(content=content).send()
    
    elif cmd == "/debug":
        if len(parts) > 1:
            setting = parts[1].lower()
            if setting == "on":
                cl.user_session.set("debug_mode", True)
                await cl.Message(content="🔍 Debug mode **enabled**. You'll see SQL queries and semantic plans.").send()
            elif setting == "off":
                cl.user_session.set("debug_mode", False)
                await cl.Message(content="🔍 Debug mode **disabled**.").send()
            else:
                await cl.Message(content="⚠️ Use `/debug on` or `/debug off`").send()
        else:
            status = "enabled" if debug_mode else "disabled"
            await cl.Message(content=f"🔍 Debug mode is currently **{status}**.").send()
    
    elif cmd == "/stats":
        stats = orchestrator.get_statistics()
        
        history_stats = f"""### 📊 Query Statistics

**Query History:**
- Total Queries: {stats['total_queries']}
- Failed: {stats['failed_queries']}
- Average Execution Time: {stats['average_execution_time_ms']:.2f}ms

**Knowledge Graph:**
- Total Concepts: {stats['knowledge_graph']['total_concepts']}
- Total Customers: {stats['knowledge_graph']['total_customers']}
- Total Mappings: {stats['knowledge_graph']['total_mappings']}
- Total Transformations: {stats['knowledge_graph']['total_transformations']}
"""
        await cl.Message(content=history_stats).send()
    
    elif cmd == "/explain":
        if len(parts) > 1:
            query = parts[1]
            try:
                explanation = orchestrator.explain_query(query)
                
                content = f"""### 📖 Query Explanation

**Query:** "{query}"

**Explanation:** {explanation['explanation']}

**Sample SQL:**
```sql
{list(explanation['sample_sql'].values())[0] if explanation['sample_sql'] else 'N/A'}
```
"""
                await cl.Message(content=content).send()
            except Exception as e:
                await cl.Message(content=f"❌ Error explaining query: {str(e)}").send()
        else:
            await cl.Message(content="⚠️ Usage: `/explain <your query>`").send()
    
    elif cmd == "/select":
        if len(parts) > 1:
            selection = parts[1].lower()
            if selection == "all":
                cl.user_session.set("selected_customers", [])
                await cl.Message(content="✅ Now querying **all customers**.").send()
            else:
                # Parse customer IDs (comma-separated)
                customer_ids = [c.strip() for c in selection.split(",")]
                available = orchestrator.list_available_customers()
                
                # Validate
                invalid = [c for c in customer_ids if c not in available]
                if invalid:
                    await cl.Message(
                        content=f"⚠️ Invalid customer IDs: {', '.join(invalid)}\n\n"
                        f"Available: {', '.join(available)}"
                    ).send()
                else:
                    cl.user_session.set("selected_customers", customer_ids)
                    await cl.Message(
                        content=f"✅ Now querying: **{', '.join(customer_ids)}**"
                    ).send()
        else:
            selected = cl.user_session.get("selected_customers", [])
            if selected:
                await cl.Message(content=f"Currently querying: **{', '.join(selected)}**").send()
            else:
                await cl.Message(content="Currently querying: **all customers**").send()
    
    elif cmd == "/limit":
        if len(parts) > 1:
            try:
                limit = int(parts[1])
                if limit < 1:
                    await cl.Message(content="⚠️ Limit must be at least 1.").send()
                elif limit > 1000:
                    await cl.Message(content="⚠️ Limit cannot exceed 1000 rows.").send()
                else:
                    cl.user_session.set("result_limit", limit)
                    await cl.Message(content=f"✅ Result limit set to **{limit} rows**.").send()
            except ValueError:
                await cl.Message(content="⚠️ Invalid number. Use `/limit <number>`").send()
        else:
            current_limit = cl.user_session.get("result_limit", 10)
            await cl.Message(content=f"Current result limit: **{current_limit} rows**").send()
    
    else:
        await cl.Message(content=f"❓ Unknown command: `{cmd}`. Type `/help` for available commands.").send()


if __name__ == "__main__":
    # This is for development only
    # Use `chainlit run app.py` to start the server
    pass
