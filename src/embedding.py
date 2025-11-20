import platform
import subprocess
import json
import sys
import docker
from src.logger import setup_logging
import time
from tqdm.auto import tqdm
import logging
import os
import uuid
from pathlib import Path
import os
from qdrant_client import QdrantClient, models
from docker.errors import ImageNotFound, APIError, NotFound

log_path = Path.cwd() / 'logs'

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("qdrant_client").setLevel(logging.WARNING)

class EmbedContent:
    def __init__(self,folder):
        self.client = QdrantClient("http://localhost:6333")
        self.collection_name = "nsechatbot-rag-sparse_dense"
        self.folder=folder
    def createCollection(self):
        if not self.client.collection_exists(self.collection_name):
            print(f"Creating Collection with name {self.collection_name}")
            self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config={
                    "bge-small-en": models.VectorParams(
                        size=384,
                        distance=models.Distance.COSINE,
                    ),
                },
                sparse_vectors_config={
                    "bm25": models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    )
                }
                ) 
            logger.info(f"Collection created successfully with the name {self.collection_name}")
        else:
           
            logger.info(f"Skipping creation of collection since {self.collection_name} already exists")
            
        return True  

    def createPoints(self)->list:
        if os.path.exists(f'{self.folder}/final_processed_circulars.json'):
            with open(f'{self.folder}/final_processed_circulars.json','r') as f:
                circulars_data= json.load(f)
        else:
            logging.error("No circulars json data found")
            return None
            

        points=[]
       
        model_handle = "BAAI/bge-small-en"
        for circular in circulars_data:
            payload = {k: circular[k] for k in circular.keys() if k != "documents"}
            for doc_entry in circular["documents"]:
                for filename, pages in doc_entry.items():
                    for page in pages:
                        table_texts = []
                        page_number = page["page_number"]
                        if page.get("page_text"):
                            for t in page.get('tables', []):
                                # combine multiple tables lists and join it into a single string
                                content = t['content']
                                if isinstance(content, list):
                                    content = "\n".join(str(item) for item in content)
                                else:
                                    content = str(content)
                                table_texts.append(content + "\n\n")
                                
                            doc_text = page['page_text'] + "\n" + "".join(table_texts)
                            page_payload = {**payload,"document_name":filename,'page_number':page_number,"content": doc_text}
                            points.append(
                                models.PointStruct(
                                    id = uuid.uuid4().hex,
                                    vector= {
                                        "bge-small-en":models.Document(text=doc_text,model=model_handle),
                                        "bm25":models.Document(text=doc_text,model="Qdrant/bm25")
                                    },
                                    
                                    payload=page_payload
                                    
                                )
                            )
        
        return points
    def createIndex(self,circulars=True):
        if circulars:
            self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="circDepartment",
                    field_schema="keyword"
                )
            self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="cirDisplayDate",
                    field_schema=models.PayloadSchemaType.DATETIME
                )
        else:
            self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="exDate",
                    field_schema=models.PayloadSchemaType.DATETIME
                )
   
    def createPointsCorpo(self):
        if os.path.exists(f'{self.folder}/corporate_actions_data.json'):
            with open(f'{self.folder}/corporate_actions_data.json','r') as f:
                ca_data= json.load(f)
        else:
            logging.error("No corporate actions json data found")
            return None


        points=[]
        for data in ca_data:
            text = data['symbol'] + "\n" + data['comp'] + "\n" + data["subject"]
            points.append(
                models.PointStruct(
                    id = uuid.uuid4().hex,
                    vector= {
                        "bge-small-en":models.Document(text=text,model="BAAI/bge-small-en"),
                        "bm25":models.Document(text=text,model="Qdrant/bm25")
                                    },      
                    payload=data
                )
            )
        return points
    def upsertPoints(self,points,desc="Embedding the PDF Circulars"):

            # Batch upsert
            BATCH_SIZE=100
            with tqdm(total=len(points),desc=desc) as progress:
                for start in range(0, len(points), BATCH_SIZE):
                    end = start + BATCH_SIZE
                    batch = points[start:end]
                    
                    self.client.upsert(
                        collection_name=self.collection_name,
                        points=batch,
                        wait=False  
                    )
                    progress.update(BATCH_SIZE)
            
    def embedData(self):
        createColl = self.createCollection()
        if not createColl:
            logger.error( "Collection could not be created")
            sys.exit(1)

        points_circ = self.createPoints()
        points_corpo = self.createPointsCorpo()
        if points_circ :
            logger.info("Qdrant points created for circulars")
            self.createIndex()
            self.upsertPoints(points=points_circ)
            logger.info("Qdrant points embedded sucessfully for circulars")
        
        if points_corpo:
            logger.info("Qdrant points created for corporate actions data")
            self.createIndex(circulars=False)
            logger.info("Index created sucussfully ..")
            self.upsertPoints(points=points_corpo,desc="Embedding corporate actions data")
            logger.info("Qdrant points embedded sucessfully for corporate actions data")

        else:
            logger.warning("No Data found to upsert")
            sys.exit(1)
if __name__ == "__main__":
  
    emb_obj = EmbedContent(folder="./data")
    emb_obj.embedData()