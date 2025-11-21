from openai import OpenAI
from qdrant_client.models import Filter, FieldCondition, MatchValue
import time
from collections import defaultdict
import os
import json
from src.processCirculars import CircularsFetchProcess
from qdrant_client import QdrantClient, models
import streamlit as st
from typing import Dict,List
from dotenv import load_dotenv
import re
from datetime import datetime as dt,timedelta
from dateparser.search import search_dates
from pandas.tseries.offsets import BDay

load_dotenv()

class RAG:
    def __init__(self,model="gemini-2.5-flash-lite"):
        self.client,self.chat_client = self.initClient()
        self.model=model
        self.provider=None
        self.max_history = 4  # Keep last 4 exchanges (4 messages: 2 user + 2 assistant)

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
    def check_bday(self,date):
        """Check if the date is business day or not . If not then add 1 day"""
        bday = BDay()
        check_bday = bday.is_on_offset(date)
        if not check_bday:
            date += BDay(1)
            
        return date
    def fetchDateRange(self,query):
        match = re.search(r'next\s+(?:(\d+)\s+days?|week|month)', query.lower())
        if any(term in query.lower() for term in ['latest', 'recent', 'new']):
            end_date = dt.today().replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = end_date - timedelta(days=15)
            start_date = start_date.isoformat()
            end_date = end_date.isoformat()
            return start_date,end_date
        elif match:
            start_date = dt.today().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
            if match.group(1):  # "next X days"
                n_days = int(match.group(1))
                end_date = start_date + timedelta(days=n_days)
            
            elif 'week' in match.group(0):  # "next week"
                days_until_monday = (7 - start_date.weekday()) % 7 or 7
                start_date = start_date + timedelta(days=days_until_monday)
                end_date = start_date + timedelta(days=6)
            
            elif 'month' in match.group(0):  # "next month" 
                next_month = start_date.replace(day=1) + timedelta(days=32)
                start_date = next_month.replace(day=1)
                end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            
            end_date = self.check_bday(end_date)
            return start_date.isoformat(),end_date.isoformat()
            
        else:
            return None,None
    def construct_qdrant_date_filter(self,start_date=None, end_date=None, exact_date=None):
        should_conditions = []

        if exact_date:
            should_conditions.append({
                "key": "exDate",
                "match": {"value": exact_date}
            })
            should_conditions.append({
                "key": "cirDisplayDate",
                "match": {"value": exact_date}
            })
        elif start_date and end_date:
            should_conditions.append({
                "key": "exDate",
                "range": {
                    "gte": start_date,
                    "lte": end_date
                }
            })
            should_conditions.append({
                "key": "cirDisplayDate",
                "range": {
                    "gte": start_date,
                    "lte": end_date
                }
            })

        if not should_conditions:
            return {"must": []}

        return {
            "should": should_conditions,
        
        }        
    def multi_stage_search(self,query: str, limit: int = 1) -> list[dict]:
        if ('corporate actions' in query.lower()) or('corporate action' in query.lower()) :
            query = query.replace('corporate actions',"Dividend,Bonus,Rights,Distribution,Buy Back,Face Value,Demerger ")
        date_search =  search_dates(query)

        start,end =self.fetchDateRange(query)


        if start and end:
            date_filter=self.construct_qdrant_date_filter(start_date=start,end_date=end)
        elif date_search:
            date = self.check_bday(date_search[0][1].replace(hour=0, minute=0, second=0, microsecond=0))
            date_iso = date.isoformat()
            date_filter=self.construct_qdrant_date_filter(exact_date=date_iso)
        
        else:
            date_filter = self.construct_qdrant_date_filter()

    
        query_points = self.client.query_points(
            collection_name="nsechatbot-rag-sparse_dense",
            prefetch=[
                models.Prefetch(
                    query=models.Document(
                        text=query,
                        model="BAAI/bge-small-en"
                    ),
                    using="bge-small-en",
                    # Prefetch ten times more results, then
                    # expected to return, so we can really rerank
                    limit=(20 * limit),
                ),
            ],
            query=models.Document(
                text=query,
                model="Qdrant/bm25", 
            ),
            using="bm25",
            limit=limit,
            with_payload=True,
            query_filter=date_filter
        )
        final = []
        
        for point in query_points.points:
            final.append(point.payload)

        pdfs_id = [res.get("id") for res in final]
        all_related_pages = []
        for pdf_id in pdfs_id:
            # Get all pages from each particular PDF
            if pdf_id:
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

        pos = 0
        for res in final:
            if 'circCategory' not in res.keys():
                all_res.insert(pos,res)
                pos += 1
                
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
        - Always identify and clearly present any corporate actions such as record dates, ex-dates, payment dates, rights issues, dividends, stock splits, etc., when mentioned in the circulars, regardless of whether the question explicitly asks about them.
        - When multiple important dates related to corporate actions (e.g., record date, ex-date, payment date) are present, list all clearly and distinctly.
        - Given a date in ISO 8601 format (e.g., 2025-12-05T00:00:00), convert it to a readable format: "Month Day, Year" (like December 5, 2025). 
            For example:
            Input: 2025-11-05T00:00:00
            Output: November 5, 2025

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

        
        context = ""
        grouped = defaultdict(list)

        # Group by document name if available, else by type placeholder
        for item in search_results:
            # Use document_name or compose a label based on type keys for grouping
            if 'document_name' in item:
                group_key = item['document_name']
            elif 'symbol' in item:
                group_key = f"Corporate Action: {item.get('symbol', 'N/A')}"
            else:
                group_key = "Unknown Document Type"
            grouped[group_key].append(item)

        # Build structured context
        for idx, (group_key, items) in enumerate(grouped.items(), 1):
            context += f"=== DOCUMENT {idx}: {group_key} ===\n\n"

            first_item = items[0]

            # Distinguish corporate actions by presence of 'symbol' and no 'content'
            if 'symbol' in first_item:
                context += f"Symbol: {first_item.get('symbol', 'N/A')}\n"
                context += f"Series: {first_item.get('series', 'N/A')}\n"
                context += f"Face Value: {first_item.get('faceVal', 'N/A')}\n"
                context += f"Subject: {first_item.get('subject', 'N/A')}\n"
                context += f"Ex-Date: {first_item.get('exDate', 'N/A')}\n"
                context += f"Company: {first_item.get('comp', 'N/A')}\n"

            # Circulars have 'content' field with document details
            elif 'content' in first_item:
                # Include circular metadata (some from first_item)
                context += f"Subject: {first_item.get('sub', 'N/A')}\n"
                context += f"Date: {first_item.get('cirDisplayDate', 'N/A')}\n"
                context += f"File Link: {first_item.get('circFilelink', 'N/A')}\n"
                context += f"Department: {first_item.get('circDepartment', 'N/A')}\n"
                context += f"Category: {first_item.get('circCategory', 'N/A')}\n"
                context += f"\nContent:\n"

                # Add all content pages in this group
                for page_num, item in enumerate(items, 1):
                    page_content = item.get('content', '').strip()
                    if page_content:
                        if len(items) > 1:
                            context += f"\n[Page {page_num}]\n"
                        context += f"{page_content}\n"

            else:
                # Generic fallback for unknown structures
                context += "No structured content found for this document.\n"

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
        
        