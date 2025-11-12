# RAG Chatbot for NSE Circulars
![Demo](media/demo.gif)
---

This project is a Retrieval-Augmented Generation (RAG) chatbot designed to answer queries related to NSE (National Stock Exchange) circulars. Using LLMs and context retrieval from official NSE circulars, the chatbot provides accurate and contextual responses to users looking for regulatory updates, compliance details, or announcements.

---
## Problem Statement

NSE circulars are often hard to track due to their volume and frequent updates, and many circulars contain complex regulatory language that can be difficult to understand. This makes it challenging for market participants to stay updated on compliance requirements and market changes in a timely manner. This project aims to simplify access to and comprehension of NSE circulars, enabling users to quickly grasp important regulatory updates and market changes, thus supporting faster and better-informed decision-making.

---
## Features

- Retrieves the latest NSE circulars from official NSE Website   
- Uses embedding search to find relevant circulars for a given query  
- Generates human-like, factual responses using an LLM  
- Understands natural language and responds with context-aware answers. 
 

---

## Tech Stack

- Qdrant for vector storage and retrieval  
- OpenAI / Gemini Models for generation  
- Streamlit for UI 
- NSE Circular PDFs

---

## System Architecture

1. **Data Ingestion** – Circulars are downloaded, parsed, and stored as text.  
2. **Vectorization** – Each document chunk is converted into embeddings using dense and sparse embeddings.  
3. **Retrieval** – For every user query, relevant chunks are retrieved from a vector database.  
4. **Generation** – The context and query are passed to an LLM to generate a comprehensive answer.  
5. **Response Delivery** – The answer is displayed on the chat interface .

---

## Setup Instructions

1. Clone the repository  
```
git clone https://github.com/yourusername/nse-rag-chatbot.git
cd nse-rag-chatbot
```

2. Create a virtual environment and install dependencies  
```
python -m venv venv
source venv/bin/activate # for Windows: venv\Scripts\activate
pip install -r requirements.txt
```
3. Add your API keys  
- Create a `.env` file and add keys such as:  
```
OPENAI_API_KEY=your_key_here
GEMINI_API_KEY=your_key_here
```

4. Prepare the database 
- To create a new database:
  - Specify the start date for circulars to be added to the database.
    ```
    python main.py --start <start_date>
    E.g python main.py --start 01-09-2025
    ```
- To update an existing database with recent information
    ```
    python main.py 
    ```
    
5. Start the application 
    ```
    streamlit run app.py 
    ```
- Additionally the script saves the circulars locally before embedding . You can change the foler path if required
    ```
    python main.py --save_path <Folder Path>
    ```
---
## Contributing

Contributions, issues, and feature requests are welcome! Feel free to fork this repository and open pull requests for improvements in forecasting accuracy, feature engineering, or bug fixes.

---
## License

This project is licensed under the MIT License.

---
## Contact

<a href="mailto:ajiteshchandra02@gmail.com">
  <img src="https://ssl.gstatic.com/ui/v1/icons/mail/rfr/logo_gmail_lockup_default_1x_r2.png" alt="Gmail" width="70" style="margin-right: 10px;" />
</a>
&nbsp;&nbsp;
<a href="https://www.linkedin.com/in/ajiteshc/">
  <img src="media/LI-In-Bug.png" alt="LinkedIn" width="30" />
</a>

---