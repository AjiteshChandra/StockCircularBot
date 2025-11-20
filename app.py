import streamlit as st
from datetime import datetime
import json
import time
from src.rag import RAG
import pandas as pd
import re
from io import StringIO
import io


# Page configuration - MUST BE FIRST
st.set_page_config(
    page_title="NSE Circular Assistant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "NSE Circular RAG Chatbot - Your AI-powered assistant for NSE regulations"
    }
)


# Enhanced Custom CSS
st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');
    
    * {
        font-family: 'Poppins', sans-serif;
    }
    
    /* Main app styling with animated gradient */
    .main {
        background: linear-gradient(-45deg, #667eea, #764ba2, #f093fb, #4facfe);
        background-size: 400% 400%;
        animation: gradientShift 15s ease infinite;
        background-attachment: fixed;
    }
    
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Chat container */
    .stChatFloatingInputContainer {
        background: rgba(255, 255, 255, 0.98);
        border-radius: 15px;
        padding: 15px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.3);
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    /* Message styling */
    .stChatMessage {
        background: rgba(255, 255, 255, 0.98);
        border-radius: 20px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.15);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.3);
        transition: transform 0.3s ease;
    }
    
    .stChatMessage:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.2);
    }
    
    /* Headers */
    h1 {
        color: white;
        text-align: center;
        font-size: 3rem;
        font-weight: 700;
        text-shadow: 3px 3px 6px rgba(0, 0, 0, 0.4);
        margin-bottom: 0.5rem;
        letter-spacing: 1px;
    }
    
    h2, h3 {
        color: white;
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
    }
    
    /* Buttons */
    .stButton>button {
        width: 100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton>button:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 25px rgba(102, 126, 234, 0.6);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    .stButton>button:active {
        transform: translateY(-1px);
    }
    
    /* Input box */
    .stChatInputContainer > div {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 15px;
        border: 2px solid #667eea;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: rgba(102, 126, 234, 0.15);
        border-radius: 10px;
        font-weight: 600;
        padding: 12px;
        transition: all 0.3s ease;
    }
    
    .streamlit-expanderHeader:hover {
        background: rgba(102, 126, 234, 0.25);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: white;
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        color: rgba(255, 255, 255, 0.9);
    }
    
    /* Success/Info/Error boxes */
    .stAlert {
        border-radius: 12px;
        backdrop-filter: blur(10px);
    }
    
    /* Divider */
    hr {
        border-color: rgba(255, 255, 255, 0.3);
        margin: 20px 0;
    }
    
    /* Spinner */
    .stSpinner > div {
        border-top-color: #667eea !important;
    }
    
    /* Welcome card animation */
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .welcome-card {
        animation: fadeInUp 0.8s ease;
    }
    </style>
""", unsafe_allow_html=True)


# Initialize RAG system
@st.cache_resource(show_spinner=False)
def load_rag_system():
    """Initialize RAG system once and cache it."""
    return RAG()


# Load RAG system
rag_system = load_rag_system()


# Function to extract markdown tables from text
def extract_tables_from_text(text):
    """Extract all markdown tables from text and convert to DataFrames"""
    tables = []
    
    # Regex to find markdown tables
    table_pattern = r'\|[^\n]+\|\n\|[-:\s\|]+\|\n(?:\|[^\n]+\|\n)+'
    matches = re.findall(table_pattern, text)
    
    for match in matches:
        try:
            # Clean and parse the table
            table_file = StringIO(match)
            df = pd.read_csv(table_file, sep='|', skipinitialspace=True)
            
            # Clean up the dataframe
            df = df.iloc[:, 1:-1]  # Remove first and last empty columns
            df.columns = df.columns.str.strip()  # Strip whitespace from column names
            df = df[df.iloc[:, 0].str.strip() != '---']  # Remove separator row
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # Strip all cells
            
            tables.append(df)
        except:
            continue
    
    return tables


# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if "total_queries" not in st.session_state:
    st.session_state.total_queries = 0
if "session_start" not in st.session_state:
    st.session_state.session_start = datetime.now()
if "welcome_shown" not in st.session_state: 
    st.session_state.welcome_shown = False


# ==================== SIDEBAR ====================
with st.sidebar:
    # Animated Logo/Header
    st.markdown("""
        <div style='text-align: center; padding: 25px 0;'>
            <div style='font-size: 4rem; margin-bottom: 10px;'>📈</div>
            <h2 style='margin: 0; font-size: 1.8rem; letter-spacing: 2px;'>NSE ASSISTANT</h2>
            
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Enhanced Stats Section
    st.markdown("### 📊 Session Statistics")
    
    # Calculate session duration
    duration = datetime.now() - st.session_state.session_start
    duration_str = f"{duration.seconds // 60}m {duration.seconds % 60}s"
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label="💬 Queries",
            value=st.session_state.total_queries,
            delta=None
        )
    with col2:
        st.metric(
            label="💭 Messages",
            value=len(st.session_state.chat_history),
            delta=None
        )
    
    st.metric(
        label="⏱️ Session Time",
        value=duration_str,
        delta=None
    )
    
    st.markdown("---")
    
    # Quick Actions with icons
    st.markdown("### 🚀 Quick Actions")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ Clear Chat", width='stretch'):
            st.session_state.chat_history = []
            st.rerun()
    
    with col2:
        if st.button("🔄 Reset Stats", width='stretch'):
            st.session_state.total_queries = 0
            st.session_state.session_start = datetime.now()
            st.rerun()
    
    # Export Chat - Always visible
    if st.button("💾 Export Chat", width='stretch'):
        if len(st.session_state.chat_history) > 0:
            chat_text = "\n\n".join([
                f"{'USER' if msg['role'] == 'user' else 'ASSISTANT'}: {msg['content']}" 
                for msg in st.session_state.chat_history
            ])
            st.download_button(
                "📥 Download Chat History",
                chat_text,
                file_name=f"nse_chat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain",
                width='stretch'
            )
        else:
            st.warning("No chat history to export yet!")
    
    st.markdown("---")
    
    # ==================== TABLE EXPORT SECTION ====================
    st.markdown("### 📊 Export Tables")

    # Extract tables only once per chat history change
    @st.cache_data
    def extract_all_tables(chat_history_str):
        """Extract tables from chat history with caching"""
        import json
        chat_history = json.loads(chat_history_str)
        all_tables = []
        for msg in chat_history:
            if msg['role'] == 'assistant':
                tables = extract_tables_from_text(msg['content'])
                all_tables.extend(tables)
        return all_tables

    # Convert chat history to string for caching
    if len(st.session_state.chat_history) > 0:
     
        chat_history_str = json.dumps(st.session_state.chat_history)
        all_tables = extract_all_tables(chat_history_str)
    else:
        all_tables = []

    if len(all_tables) > 0:
        st.success(f"Found {len(all_tables)} table(s)")
        
        # Initialize table_index in session state
        if 'selected_table_index' not in st.session_state:
            st.session_state.selected_table_index = 0
        
        # Select which table to download
        if len(all_tables) > 1:
            table_index = st.selectbox(
                "Select table:",
                range(len(all_tables)),
                format_func=lambda x: f"Table {x + 1} ({all_tables[x].shape[0]}×{all_tables[x].shape[1]})",
                key="table_selector"
            )
        else:
            table_index = 0
            st.info(f"Table: {all_tables[0].shape[0]} rows × {all_tables[0].shape[1]} columns")
        
        # Preview the selected table - NO EXPANDER, direct display
        st.markdown("**📋 Preview:**")
        st.dataframe(all_tables[table_index], width="stretch", height=200)
            
        # Download options
        col1, col2 = st.columns(2)
        
        with col1:
            # Download as CSV
            csv = all_tables[table_index].to_csv(index=False).encode('utf-8')
            st.download_button(
                "📄 CSV",
                csv,
                file_name=f"nse_table_{table_index+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                width='stretch'
            )
        
        with col2:
            # Download as Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                all_tables[table_index].to_excel(writer, sheet_name='NSE Data', index=False)
            
            st.download_button(
                "📊 Excel",
                buffer.getvalue(),
                file_name=f"nse_table_{table_index+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width='stretch'
            )
        
        # Option to download all tables
        if len(all_tables) > 1:
            if st.button("📥 Download All Tables", width='stretch'):
                buffer_all = io.BytesIO()
                with pd.ExcelWriter(buffer_all, engine='xlsxwriter') as writer:
                    for idx, table in enumerate(all_tables):
                        table.to_excel(writer, sheet_name=f'Table_{idx+1}', index=False)
                
                st.download_button(
                    "📥 Get All Tables (Excel)",
                    buffer_all.getvalue(),
                    file_name=f"nse_all_tables_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch'
                )
    else:
        st.info("No tables found in chat")
    
    st.markdown("---")
    
    # Example Queries with better formatting
    with st.expander("💡 Example Questions", expanded=False):
        examples = [
            "📋 List all corporate actions in next 3 days",
            "📜 Recent SEBI regulations on derivatives",
            "💼 What is the T+1 settlement cycle implementation?",
            "🔍 Latest circular on mutual funds"
        ]
        for example in examples:
            st.markdown(f"• {example}")
    
    st.markdown("---")
    
    # Tips Section
    with st.expander("🎯 Tips for Better Results"):
        st.markdown("""
        - **Be specific** in your questions
        - Use **exact terms** from circulars
        - Ask about **recent regulations** for latest info
        - Mention **dates** if looking for specific period
        """)
    
    st.markdown("---")
    
    # Footer with timestamp
    st.markdown(f"""
        <div style='text-align: center; padding: 15px 0; font-size: 0.75rem; opacity: 0.7;'>
            <p>🤖 AI-Powered Assistant</p>
            <p style='margin-top: 5px;'>Last updated: {datetime.now().strftime('%d %b %Y')}</p>
        </div>
    """, unsafe_allow_html=True)


# MAIN CONTENT 

# Hero Section with gradient text
st.markdown("""
    <div style='text-align: center; padding: 30px 0 20px 0;'>
        <h1 style='background: linear-gradient(135deg, #fff 0%, #e0e7ff 100%); 
                   -webkit-background-clip: text; 
                   -webkit-text-fill-color: transparent;
                   background-clip: text;'>
            🔍 NSE Regulatory Assistant
        </h1>
        <p style='color: white; font-size: 1.2rem; opacity: 0.95; margin-top: 10px; font-weight: 300;'>
            Get instant, accurate answers about NSE regulations, circulars, and corporate actions
        </p>
    </div>
""", unsafe_allow_html=True)


# Chat message styling
st.html("""
    <style>
    div[data-testid="stChatMessageContent"] {
        background-color: transparent !important;
    }
    
    div[data-testid="stChatMessage"] {
        background-color: transparent !important;
    }
    </style>
""")


# Welcome message streaming function
def stream_welcome_tokens():
    message = "👋 Hi! Ask me anything about NSE circulars, regulations, compliance and corporate actions."
    words = message.split()
    for word in words:
        yield word + " "
        time.sleep(0.08)



# Handle new user input 
question = st.chat_input("💬 Ask me anything about NSE circulars or corporate actions ...", key="chat_input")

#  Welcome message (only if chat is empty)
if len(st.session_state.chat_history) == 0 and not st.session_state.welcome_shown:
    with st.chat_message("assistant", avatar="🤖"):
        st.write_stream(stream_welcome_tokens())
    st.session_state.welcome_shown = True

#  Display ALL existing chat messages
for message in st.session_state.chat_history:
    avatar = "🧑‍💼" if message["role"] == "user" else "🤖"
    with st.chat_message(message["role"], avatar=avatar):
        st.markdown(message["content"])

# Process NEW query 
if question:
    st.session_state.total_queries += 1
    
    # Add user message to history ONCE
    st.session_state.chat_history.append({"role": "user", "content": question})
    
    # Display the new user message
    with st.chat_message("user", avatar="🧑‍💼"):
        st.markdown(question)
    
    # Show spinner OUTSIDE chat_message
    
        
    
    # Generate and display assistant response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("🔍 Searching..."):
            time.sleep(0.5)
            response_placeholder = st.empty()
            full_response = ""
            
            try:
            
                for chunk in rag_system.rag_streaming(question, st.session_state.chat_history, top_k=15):
                    full_response += chunk
                    response_placeholder.markdown(full_response + "▌")
                
                response_placeholder.markdown(full_response)
                
                # Add assistant response
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": full_response
                })
                
            except Exception as e:
                error_msg = f"❌ **Error occurred:** {str(e)}\n\nPlease try again or rephrase your question."
                st.error(error_msg)
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": error_msg
                })
# Floating action button for scrolling to top
if len(st.session_state.chat_history) > 5:
    st.markdown("""
        <a href='#nse-circular-assistant' style='
            position: fixed;
            bottom: 100px;
            right: 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            width: 50px;
            height: 50px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            text-decoration: none;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
            font-size: 1.5rem;
            transition: all 0.3s ease;
            z-index: 999;
        ' onmouseover='this.style.transform="scale(1.15)"' 
           onmouseout='this.style.transform="scale(1)"'>
            ↑
        </a>
    """, unsafe_allow_html=True)
