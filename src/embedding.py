import platform
import subprocess
import json
import docker
from src.logger import setup_logging
import time
from tqdm.auto import tqdm
import logging
import uuid
from pathlib import Path
import os
from qdrant_client import QdrantClient, models
from docker.errors import ImageNotFound, APIError, NotFound

log_path = Path.cwd() / 'logs'
setup_logging("embedding", log_dir=log_path,to_console=True,console_filter_keywords=["Failed","Successfully","Error",'ready'])
logger = logging.getLogger(__name__)
class LaunchQdrant:

    def start_qdrant(self,storage_path='./qdrant_storage', rest_port=6333, grpc_port=6334,container_name='qdrant'):
        try:
            client = docker.from_env()
            
            # Create storage directory if it doesn't exist
            os.makedirs(storage_path, exist_ok=True)
            absolute_path = os.path.abspath(storage_path)
            
            # Check if container already exists
            try:
                existing_container = client.containers.get(container_name)
                print(f"Container '{container_name}' already exists")
                
                if existing_container.status == 'running':
                    print("Container is already running")
                    print(f"  Dashboard: http://localhost:{rest_port}/dashboard")
         
                    return existing_container
                else:
                    print("Starting existing container...")
                    existing_container.start()
                    print(f"  Dashboard: http://localhost:{rest_port}/dashboard")
              
                    return existing_container
            except NotFound:
                print("Container not found, creating new one...")
            
            # Pull latest Qdrant image
            print("Pulling Qdrant image...")
            logger.info("Pulling Qdrant image...")
            client.images.pull('qdrant/qdrant:latest')
            
            # Run Qdrant container
            container = client.containers.run(
                image='qdrant/qdrant:latest',
                name=container_name,
                detach=True,
                ports={
                    '6333/tcp': rest_port,
                    '6334/tcp': grpc_port
                },
                volumes={
                    f'{container_name}_data': {
                        'bind': '/qdrant/storage',
                        'mode': 'rw'
                    }
                },
                environment={
                    'QDRANT__SERVICE__GRPC_PORT': str(grpc_port)
                },
                restart_policy={"Name": "no"}
            )
            
            print(f"✓ Qdrant container started successfully!")
            print(f"  Container ID: {container.short_id}")
            print(f"  Dashboard: http://localhost:{rest_port}/dashboard")
            logger.info("Qdrant service started successfully")
            return container
            
        except APIError as e:
            print(f"Docker API error: {e}")
            return None
        except Exception as e:
            print(f"Error starting Qdrant: {e}")
            return None


class EmbedContent:
    def __init__(self):
        self.client = QdrantClient("http://localhost:6333")
        self.collection_name = "nsechatbot-rag-sparse_dense"
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
        else:
            print(f"Skipping creation of collection since {self.collection_name} already exists")
            
        return True  

    def createPoints(self)->list:
        with open('data/final_processed_circulars.json','r') as f:
            circulars_data= json.load(f)

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
                                # If t['content'] is a list, join it into a single string
                                content = t['content']
                                if isinstance(content, list):
                                    content = "\n".join(str(item) for item in content)
                                else:
                                    content = str(content)
                                table_texts.append(content + "\n\n")
                                
                            doc_text = page['page_text'] + "\n" + "".join(table_texts)
                            page_payload = {**payload,'page_number':page_number,"content": doc_text}
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
    def upsertPoints(self,points):
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
            # Batch upsert
            BATCH_SIZE=100
            with tqdm(total=len(points),desc="Embedding the PDF data") as progress:
                for start in range(0, len(points), BATCH_SIZE):
                    end = start + BATCH_SIZE
                    batch = points[start:end]
                    
                    self.client.upsert(
                        collection_name=self.collection_name,
                        points=batch,
                        wait=False  # use True if you want to block until indexing finishes
                    )
                    progress.update(BATCH_SIZE)
            
    def embedData(self):
        createColl = self.createCollection()
        if not createColl:
            return "Collection could not be created"
        points = self.createPoints()
        self.upsertPoints(points=points)

if __name__ == "__main__":
    # obj = LaunchQdrant()
    # obj.start_docker_service()
    # obj.start_qdrant()

    emb_obj = EmbedContent()
    emb_obj.embedData()