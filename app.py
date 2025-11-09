import streamlit as st
from qdrant_client import QdrantClient
import time

# Page configuration
st.set_page_config(
    page_title="RAG Chatbot",
    page_icon="🤖",
    layout="centered"
)

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
# --- Initialize session state so message shows once ---
if "welcome_done" not in st.session_state:
    st.session_state.welcome_done = False
# Configuration in sidebar
# with st.sidebar:
#     st.header("Configuration")
    
#     qdrant_url = st.text_input("Qdrant URL", value="http://localhost:6333")
#     collection_name = st.text_input("Collection Name", value="my_documents")
    
#     top_k = st.slider("Documents to retrieve", 1, 10, 3)

# Initialize clients
@st.cache_resource
def get_qdrant_client(url):
    return QdrantClient(url=url)



# Main UI
st.title("💬 RAG Chatbot")

# Display chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

def display_token_stream(text, delay=0.07):
    placeholder = st.empty()
    displayed_text = ""
    for token in text.split():
        displayed_text += token + " "
        placeholder.markdown(f"**{displayed_text.strip()}**")
        time.sleep(delay)

# --- Run on first page load only ---
if not st.session_state.welcome_done:
    welcome_message = "Hello there! 👋 Welcome to the chatbot interface."
    display_token_stream(welcome_message, delay=0.1)
    st.session_state.welcome_done = True
else:
    st.write("Hello again! 👋")

# Chat input
if question := st.chat_input("Ask a question..."):
    # Add user message
    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Searching..."):
            try:
                # Connect to Qdrant
                client = get_qdrant_client(qdrant_url)
                embedding_model = get_embedding_model()
                
                # Search Qdrant
                query_embedding = embedding_model.encode(question).tolist()
                search_results = client.search(
                    collection_name=collection_name,
                    query_vector=query_embedding,
                    limit=top_k
                )
                
                # Display results
                if search_results:
                    st.markdown("**Retrieved Documents:**")
                    for i, result in enumerate(search_results, 1):
                        with st.expander(f"Document {i} (Score: {result.score:.4f})"):
                            st.write(result.payload.get('text', 'No text available'))
                    
                    # Simple answer from top result
                    answer = f"Based on the search, here's the most relevant information:\n\n{search_results[0].payload.get('text', 'No text available')}"
                    st.markdown(answer)
                else:
                    answer = "No relevant documents found."
                    st.warning(answer)
                
                # Add to chat history
                st.session_state.chat_history.append({"role": "assistant", "content": answer})
                
            except Exception as e:
                st.error(f"Error: {str(e)}")