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


def format_result_table(result) -> str:
    """Format harmonized result as a markdown table.
    
    Args:
        result: HarmonizedResult object
        
    Returns:
        Markdown formatted table
    """
    if not result.results:
        return "*No results found*"
    
    # Get column names from first row
    if not result.results:
        return "*No data*"
    
    first_row = result.results[0]
    columns = list(first_row.data.keys())
    
    # Build markdown table
    lines = []
    
    # Header
    header = "| " + " | ".join(columns) + " |"
    separator = "|" + "|".join(["---" for _ in columns]) + "|"
    lines.append(header)
    lines.append(separator)
    
    # Rows (limit to first 50 to avoid huge messages)
    for row in result.results[:50]:
        values = [str(row.data.get(col, "")) for col in columns]
        line = "| " + " | ".join(values) + " |"
        lines.append(line)
    
    if len(result.results) > 50:
        lines.append(f"\n*... and {len(result.results) - 50} more rows*")
    
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
        f"- **Success Rate:** {stats['success_rate']:.1f}%",
        f"- **Total Rows:** {stats['total_rows']}",
        f"- **Customers Queried:** {len(stats['customers_queried'])}",
        f"- **Customers Succeeded:** {len(stats['customers_succeeded'])}",
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
    lines = [
        "### 🔍 Debug Information",
        "",
        "**Semantic Plan:**",
        f"- Intent: `{debug['semantic_plan']['intent']}`",
        f"- Projections: {', '.join(f'`{p}`' for p in debug['semantic_plan']['projections'])}",
    ]
    
    if debug['semantic_plan']['filters']:
        lines.append(f"- Filters: {len(debug['semantic_plan']['filters'])}")
        for f in debug['semantic_plan']['filters']:
            lines.append(f"  - `{f['concept']}` {f['operator']} `{f['value']}`")
    
    if debug['semantic_plan']['aggregations']:
        lines.append(f"- Aggregations: {', '.join(debug['semantic_plan']['aggregations'])}")
    
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
        
        mode = "LLM mode" if orchestrator.use_llm else "Mock mode"
        
        # Send welcome message
        welcome_msg = f"""# 🎯 Schema Translator Chat

Welcome! I'm running in **{mode}**.

I can help you query customer databases using natural language. Just ask me questions like:
- "Show me all contracts"
- "Find active contracts with value over 10000"
- "Count contracts by status"
- "List all customers"

### Available Commands:
- `/customers` - List available customers
- `/debug on/off` - Toggle debug mode
- `/stats` - Show query statistics
- `/help` - Show this help message

Try asking me a question!
"""
        
        await cl.Message(content=welcome_msg).send()
        
        # Get available customers
        customers = orchestrator.list_available_customers()
        logger.info(f"Chat started with {len(customers)} customers available")
        
    except Exception as e:
        logger.error(f"Failed to initialize orchestrator: {e}", exc_info=True)
        await cl.Message(
            content=f"❌ **Error:** Failed to initialize. Please check configuration.\n\n`{str(e)}`"
        ).send()


@cl.on_message
async def main(message: cl.Message):
    """Handle incoming chat messages."""
    global orchestrator
    
    orchestrator = cl.user_session.get("orchestrator")
    debug_mode = cl.user_session.get("debug_mode", False)
    selected_customers = cl.user_session.get("selected_customers", [])
    
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
            
            # Results table
            content_parts.append("### ✅ Query Results")
            content_parts.append(format_result_table(result))
            
            # Statistics
            stats = {
                'success_rate': result.success_rate,
                'total_rows': result.total_count,
                'customers_queried': result.customers_queried,
                'customers_succeeded': result.customers_succeeded,
                'customers_failed': result.customers_failed,
                'execution_time_ms': response['execution_time_ms']
            }
            content_parts.append("\n" + format_statistics(stats))
            
            # Debug info if enabled
            if debug_mode and 'debug' in response:
                content_parts.append("\n" + format_debug_info(response['debug']))
            
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
- "Count contracts by status"
- "List contracts with value over 10000"
- "Show contracts expiring soon"

**Commands:**
- `/customers` - List available customers
- `/select <customer_id>` - Query specific customer(s)
- `/select all` - Query all customers (default)
- `/debug on/off` - Toggle debug mode
- `/stats` - Show query statistics
- `/explain <query>` - Explain how a query will be processed
- `/help` - Show this help message

**Tips:**
- Use natural language - I'll understand it!
- Be specific about what you want to see
- Use filters like "active", "over 1000", "expiring soon"
- Enable debug mode to see SQL queries
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
- Successful: {stats['successful_queries']} ({stats['success_rate']:.1f}%)
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
    
    else:
        await cl.Message(content=f"❓ Unknown command: `{cmd}`. Type `/help` for available commands.").send()


if __name__ == "__main__":
    # This is for development only
    # Use `chainlit run app.py` to start the server
    pass
