"""
Gradio wrapper for Chainlit app to work on HF Spaces.
This provides HTTP-based interface instead of WebSocket.
"""
import gradio as gr
from schema_translator.orchestrator import ChatOrchestrator
from schema_translator.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize orchestrator
config = Config()
orchestrator = ChatOrchestrator(config, use_llm=True)

def process_query(message: str, history: list) -> str:
    """Process user query and return response."""
    try:
        logger.info(f"Processing query: {message}")
        result = orchestrator.process_query(message)
        
        if result.get("error"):
            return f"❌ Error: {result['error']}"
        
        # Format results
        response = f"**Query Intent:** {result.get('intent', 'Unknown')}\n\n"
        
        if result.get("results"):
            total_rows = sum(len(r.get("data", [])) for r in result["results"])
            success_rate = result.get("success_rate", 0) * 100
            
            response += f"📊 **Results:** {total_rows} rows from {len(result['results'])} customers\n"
            response += f"✅ **Success Rate:** {success_rate:.1f}%\n\n"
            
            # Show sample data
            for customer_result in result["results"][:3]:  # Show first 3 customers
                customer = customer_result.get("customer_id", "Unknown")
                data = customer_result.get("data", [])
                
                if data:
                    response += f"\n**{customer.upper()}** ({len(data)} rows):\n"
                    for row in data[:5]:  # Show first 5 rows
                        response += f"• {row}\n"
        else:
            response += "*No results found*"
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return f"❌ Error: {str(e)}"

# Create Gradio interface
with gr.Blocks(title="Schema Translator", theme=gr.themes.Soft()) as demo:
    gr.Markdown("""
    # 🔄 Schema Translator
    
    Query multiple customer databases using natural language. The system automatically:
    - Understands your query intent
    - Maps to different customer schemas
    - Executes across all databases
    - Harmonizes and presents results
    
    **Try queries like:**
    - "Show me all active contracts"
    - "How many contracts expire this month?"
    - "What's the total contract value?"
    """)
    
    chatbot = gr.Chatbot(height=400, label="Schema Translator")
    msg = gr.Textbox(
        label="Your Query",
        placeholder="Enter your natural language query...",
        lines=2
    )
    
    with gr.Row():
        submit = gr.Button("Send", variant="primary")
        clear = gr.Button("Clear")
    
    gr.Markdown("""
    ---
    **Available Customers:** A, B, C, D, E, F | **Powered by:** Claude Sonnet 4.5
    """)
    
    def respond(message, chat_history):
        bot_message = process_query(message, chat_history)
        chat_history.append((message, bot_message))
        return "", chat_history
    
    msg.submit(respond, [msg, chatbot], [msg, chatbot])
    submit.click(respond, [msg, chatbot], [msg, chatbot])
    clear.click(lambda: None, None, chatbot, queue=False)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
