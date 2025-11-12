from openai import OpenAI
from qdrant_client.models import Filter, FieldCondition, MatchValue
import time
from collections import defaultdict
import os
from qdrant_client import QdrantClient, models
import streamlit as st
from typing import Dict,List
from dotenv import load_dotenv

load_dotenv()

class RAG:
    def __init__(self,model="gemini-2.5-flash-lite"):
        self.client,self.chat_client = self.initClient()
        self.model=model
        self.provider=None
        self.max_history = 4  # Keep last 6 exchanges (6 messages: 3 user + 3 assistant)

    def getKey(self):
        openai_key = os.getenv("OPENAI_API_KEY")
        gemini_key = os.getenv("GEMINI_API_KEY")

        if openai_key:
            print("Detected OpenAI API key.")
            provider = "openai"
            api_key = openai_key
        elif gemini_key:
            print("Detected Gemini API key.")
            provider = "gemini"
            api_key = gemini_key
        else:
            print("No API key detected.")
            provider = None
            api_key = None

        return provider, api_key
    def initClient(self):
        client = QdrantClient("http://localhost:6333")
        self.provider, api_key = self.getKey()
        if self.provider == "openai":
            try:
                chat_client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=api_key,
                        )
                return client,chat_client
            except:
                return False
        else:
            chat_client = OpenAI(
            api_key=api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
    )
            return client,chat_client
            
    def multi_stage_search(self,query: str, limit: int = 1) -> List[dict]:
        results = self.client.query_points(
            collection_name="nsechatbot-rag-sparse_dense",
            prefetch=[
                models.Prefetch(
                    query=models.Document(
                        text=query,
                        model="BAAI/bge-small-en",
                    ),
                    using="bge-small-en",
                    # Prefetch fifteen times more results, then
                    # expected to return, so we can really rerank
                    limit=(16 * limit),
                ),
            ],
            query=models.Document(
                text=query,
                model="Qdrant/bm25", 
            ),
            using="bm25",
            limit=limit,
            with_payload=True,
        )
        final = []
        
        for point in results.points:
            final.append(point.payload)

        pdfs_id = [res.get("id") for res in final]
        final.sort(key=lambda x:x['cirDisplayDate'],reverse=True)
        all_related_pages = []
        for pdf_id in pdfs_id:
            # Get all pages from each particular PDF
            pdf_pages = self.client.scroll(
                collection_name="nsechatbot-rag-sparse_dense",
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key='id',
                            match=MatchValue(value=pdf_id)
                        )
                    ]
                ),
                limit=5  ,
                with_payload=True,
                with_vectors=False  
            )
            all_related_pages.extend(pdf_pages[0])  
            
        all_related_pages.sort(key=lambda x: (x.payload["id"], x.payload["page_number"]))
        all_related_pages.sort(key=lambda x:x.payload["cirDisplayDate"],reverse=True)
        all_res = []
        for res in all_related_pages:
            all_res.append(res.payload)

        return all_res
    def get_unique_circulars_with_all_pages(self,circulars, n=5):
        """
        Returns n unique circulars with ALL their pages included.
        Groups by circular identifier, keeping all pages per circular.
        """
        # Group all pages by circular identifier
        circular_groups = defaultdict(list)
        
        for circular in circulars:
            # Use file link as unique circular identifier
            circular_id = circular.get('circFilelink', '')
            circular_groups[circular_id].append(circular)
        
        # Get first n unique circulars with all their pages
        result = []
        for circular_id in list(circular_groups.keys())[:n]:
            result.extend(circular_groups[circular_id])
        
        return result
    
    def build_prompt(self,query, search_results):
        prompt_template = """You are an expert assistant specializing in NSE (National Stock Exchange of India) circulars.

        ### INSTRUCTIONS
        - Answer using ONLY the information from the circular excerpts provided below.
        - Provide answers in a direct, natural conversational style as if the information is your own knowledge.
        - Do NOT mention document names, circular numbers, excerpts, or references in your response body.
        - Do NOT use phrases like "based on the provided circulars", "according to the documents", "CIRCULAR X states", or similar meta-references.
        - If multiple excerpts are from the same source, combine them coherently without citing the source.
        - Present information clearly and directly without repeatedly citing document structure.
        - When data is comparative or structured, use markdown tables for better readability.
        - Extract and present only relevant information. Reproduce full tables only when necessary for clarity.
        - Use the most recent information when there are conflicting details across different circulars.
        - Maintain a factual, neutral tone and speak authoritatively about the information.
        

        ### CLASSIFICATION GUIDELINES
        - Non-Business Days refer ONLY to calendar dates or days when markets/operations are closed
        - Securities, funds, and financial instruments are NEVER categories of days
        - When answering questions about business days, focus exclusively on temporal information
        - Distinguish between: (1) What is being discussed (e.g., mutual funds), and (2) When it applies (e.g., business days)

        ### STRICT RULES
        1. Do NOT use external knowledge or make assumptions beyond what's provided.
        2. Do NOT modify stock symbols, index names, or any codes - use them exactly as written.
        3. Do NOT invent data or speculate.
        4. If URLs are mentioned in the content, output them as plain text (no markdown/HTML formatting).
        5. Avoid repeating the same information multiple times.
        6. NEVER reference the document structure, excerpt numbers, or circular labels in your answer.
        7. Write as if you naturally know this information - do not mention your sources or say "the circular states" or "according to the document".
        8. If NO relevant information is found in the provided context after thorough review, respond with EXACTLY: "The provided circulars do not contain this information."
        9. Otherwise, provide a direct answer without any meta-commentary about where the information came from.

        ### REASONING PROCESS
        Before answering:
        1. Identify which excerpts relate to the question
        2. Extract key facts from those excerpts
        3. Synthesize the information into a clear answer
        4. Verify your answer is supported by the provided context

        ### VALIDATION CHECKLIST
        Before finalizing your answer, verify:
        - Have I confused an entity type (fund, security, index) with a time classification?
        - Does my answer logically match the question category?
        - If the question asks about dates/schedules, is my answer exclusively about temporal information?
        - If answering about mutual funds, am I describing fund categories/characteristics (correct) or treating them as time periods (incorrect)?

        ### CONTEXT
        {context}

        ### QUESTION
        {query}

        

        
        ### ANSWER""".strip()

        
        # Build context with clear separation and better structure
        context = ""
    

        grouped = defaultdict(list)

        # Group by document name
        for circular in search_results:
            doc_name = circular.get("document_name", 'N/A')
            grouped[doc_name].append(circular)

        # Build structured context
        for idx, (doc_name, pages) in enumerate(grouped.items(), 1):
            # Document header
            context += f"=== CIRCULAR {idx}: {doc_name} ===\n\n"
            
            # Metadata (only once per document)
            if pages:
                first_page = pages[0]
                if first_page.get('sub'):
                    context += f"Subject: {first_page.get('sub', 'N/A')}\n"
                if first_page.get('cirDisplayDate'):
                    context += f"Date: {first_page.get('cirDisplayDate', 'N/A')}\n"
                if first_page.get('circFilelink'):
                    context += f"File Link: {first_page.get('circFilelink', 'N/A')}\n"
            
            context += f"\nContent:\n"
            
            # Add content from each page with clear page markers
            for page_num, page in enumerate(pages, 1):
                page_content = page.get('content', '').strip()
                if page_content:
                    if len(pages) > 1:
                        context += f"\n[Page {page_num}]\n"
                    context += f"{page_content}\n"
            
            # Document separator
            context += f"\n{'='*60}\n\n"


        
        return prompt_template.format(query=query, context=context).strip()
    
    def rag_streaming(self, query,chat_history, top_k=15):

        search_results = self.multi_stage_search(query, top_k)
        results = self.get_unique_circulars_with_all_pages(search_results)
        prompt = self.build_prompt(query, results)
        # Build messages list with history
        messages = []
        
        # Add recent chat history (limit to last N messages)
        messages.extend(chat_history[-self.max_history:])
        
        # Add current user query
        messages.append({"role": "user", "content": prompt})
        
        # Stream response
        stream = self.chat_client.chat.completions.create(
            model=self.model,
            messages=messages,
            stream=True
        )
        
        stop_phrase = "The provided circulars do not contain this information."
        output = ""
        
        for event in stream:
            if event.choices[0].delta.content is not None:
                chunk = event.choices[0].delta.content
                output += chunk
                yield chunk
                
                if stop_phrase in output:
                    break
        
        