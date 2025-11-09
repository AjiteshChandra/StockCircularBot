from nsepython import nsefetch
import io
from datetime import datetime as dt
from pathlib import Path
import pdfplumber
import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
from collections import defaultdict
import requests
from src.logger import setup_logging
from tabulate import tabulate
from zipfile import ZipFile
import uuid
import hashlib

log_path = Path.cwd() / 'logs'
setup_logging("Data_fetch", log_dir=log_path,to_console=True,console_filter_keywords=["Failed","Successfully","Error",'ready'])
logger = logging.getLogger(__name__)

class CircularsFetchProcess:
    def __init__(self,start_date:str,end_date:str|None=None,folder:str="data/"):
        self.start_date=start_date
        self.end_date=end_date
        self.folder=folder

    def convert_to_rfc(self,date):
        
        date_str = date
        
        # Parse the string into a datetime object
        date_obj = dt.strptime(date_str, "%B %d, %Y")
        
        
        # Convert to RFC 3339 / ISO 8601 format string
        rfc_date_str = date_obj.isoformat()
        
        return  rfc_date_str # Output: 2025-10-25T00:00:00+00:00
    
    def parse_dates(self,circulars):
        circulars_data = circulars.copy()
        for circular in circulars_data:
            circular['cirDisplayDate'] = self.convert_to_rfc(circular['cirDisplayDate'])

        return circulars_data
    
    def map_progress(self,pool, seq, f,desc):
        results = []

        with tqdm(total=len(seq),desc=desc) as progress:
            futures = []

            for el in seq:
                future = pool.submit(f, el)
                future.add_done_callback(lambda p: progress.update())
                futures.append(future)

            for future in futures:
                result = future.result()
                results.append(result)

        return results
    def removeDuplicateCirculars(self,circulars):
        seen = set()
        unique = []
        for d in circulars:
            if d["circFilelink"] not in seen:
                seen.add(d["circFilelink"])
                unique.append(d)
        return unique
    
    def get_all_circulars(self):
        if not self.end_date:
            self.end_date= dt.today().strftime("%d-%m-%Y")

        nsefetch("https://www.nseindia.com")
        url = f'https://www.nseindia.com/api/circulars?fromDate={self.start_date}&toDate={self.end_date}'
        circulars = nsefetch(url)
        final_circulars = [{k: v for k, v in i.items() if k not in ('circFileSize','circDisplayNo','cirDate','fileExt')} for i in circulars["data"]]
        final_circulars = self.removeDuplicateCirculars(final_circulars)
        final_circulars = self.parse_dates(final_circulars)
        final_circulars = [d for d in final_circulars if not (d["circFilename"].endswith(".null"))]
        return final_circulars
    
    def retry(self,circulars):
        n = len(circulars)
        cnt = 4 # Number of retries
        while cnt != 0:
            count_pdf_files = sum(1 for f in Path(f"{self.folder}/pdfs").iterdir() if f.is_file())
            count_zip_files = sum(1 for f in Path(f"{self.folder}/zips").iterdir() if f.is_file())
            total_circ=count_pdf_files + count_zip_files
            if (total_circ < n):
                logger.info(f"Retrying to download all the circulars {n-total_circ} were missing/not downloaded")
                self.download_circulars(circulars,desc="Retrying to fetch all NSE circulars")
                cnt -= 1
            else:
                return

        return
    
    def download_circulars(self,circulars_list:list[dict],desc="Fetching NSE Circulars.."):
        folder = Path(self.folder)
        (folder/'pdfs').mkdir(parents=True, exist_ok=True)
        (folder/'zips').mkdir(parents=True, exist_ok=True)
        
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/XXXX Safari/537.36",
        # "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",  
        "Origin": "https://www.nseindia.com",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",

        }
        # Create a session to manage cookies
        session = requests.Session()

        for circular in tqdm(circulars_list,desc=desc):
            url = circular['circFilelink']
            file_name = circular['circFilename']
            # Checking if the file already exists 
            if not (Path(f"{self.folder}/pdfs/{file_name}.bin").exists() or Path(f"{self.folder}/zips/{file_name}.bin").exists()):
                # Make initial request to get cookies set
            
                response = session.get(url,headers=headers)
                if response.status_code == 200:
                        if circular['circFilelink'].endswith('.pdf'):
                            with open(f'data/pdfs/{file_name}', 'wb') as f:
                                f.write(response.content)
                            
                        elif circular['circFilelink'].endswith('.zip'):
                            with open(f'data/zips/{file_name}', 'wb') as f:
                                f.write(response.content)
                
       
        tqdm.write("")

    def generate_table_id(self,json_obj:dict,table_number,pg_no):
        combined = f"{json_obj['fileDept']}-{json_obj['circNumber']}-{json_obj['circCategory']}-{str(table_number)}-{str(pg_no)}"
        hash_object = hashlib.md5(combined.encode())
        hash_hex = hash_object.hexdigest()
        table_id = hash_hex
        return table_id
    
    def extractZipContent(self,json,file):
        zip_data = open(file, "rb").read()
        zip_buffer = io.BytesIO(zip_data)
        with ZipFile(zip_buffer, "r") as zf:
            final={}
            for name in zf.namelist():
                pdf_texts = [] 
                if name.lower().endswith(".pdf"):
                    # Read PDF file bytes
                    pdf_bytes = zf.read(name)
        
                    # Open PDF directly from memory
                    pdf_file = io.BytesIO(pdf_bytes)
                    text = self.extract_text_and_tables(pdf_file,json)
                    # pdf_texts.append(text)
                    final.update({Path(name).stem+'.pdf': text})

        return final
    
    def getTables(self,json,page):
        """Extract Tables from all the pages in PDF"""
        all_tables=[]
        tables= page.find_tables()
        ## Skipping the circular ref table(since its already present in metadata)
        start = 0 if page.page_number > 1 else 1
        for table in range(start,len(tables)):
            table_id=self.generate_table_id(json,table_number=table,pg_no=page.page_number)
            cl =  tables[table].extract()

            #Replacing "\n" from text since it was present for layout inside PDF pages
            cl =[[cell.replace("\n", " ").replace("·", "") if cell else "" for cell in row] for row in cl]
            #converting it to markdown table 
            # table_cleaned= tabulate(cl[1:], headers=cl[0], tablefmt="github")
            table_dict= {'table_id':table_id,"content":cl}
            all_tables.append(table_dict)


        return all_tables if len(all_tables) > 0 else []
    
    def extract_text_and_tables(self,file,json):
        all_page_text= []
        
        try:
            # filename = Path(json['circFilename'])
            # file = Path(pdf_path)/filename
            with pdfplumber.open(file) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    tables= page.find_tables()
                    table_bboxes = [table.bbox for table in page.find_tables()]
                    words = page.extract_words()
                    outside_words = []
                    for word in words:
                        x0, top, x1, bottom = word['x0'], word['top'], word['x1'], word['bottom']
                        inside_table = any(x0 >= bbox[0] and x1 <= bbox[2] and top >= bbox[1] and bottom <= bbox[3] for bbox in table_bboxes)
                        if not inside_table:
                            outside_words.append(word)
                    
                    # Group words by line using a threshold for top coordinate
                    lines = defaultdict(list)
                    for word in outside_words:
                        # Find close line by top coordinate
                        placed = False
                        for line_top in lines:
                            if abs(line_top - word['top']) < 3:  # 3-pixel threshold
                                lines[line_top].append(word)
                                placed = True
                                break
                        if not placed:
                            lines[word['top']].append(word)
                    
                    # Sort lines by vertical position
                    sorted_lines = sorted(lines.items(), key=lambda x: x[0])
                    
                    # Construct text with newlines
                    outside_text = ""
                    for _, line_words in sorted_lines:
                        # Sort words in line by x0 (horizontal position)
                        line_words.sort(key=lambda w: w['x0'])
                        line_text = " ".join(w['text'] for w in line_words)
                        outside_text += line_text + "\n"

                    tables = self.getTables(json,page)
                    page_text = {'page_number':page.page_number,"page_text":outside_text,'tables':tables}
                    all_page_text.append(page_text)

                
        except FileNotFoundError:
            return None
            
        return all_page_text
    
    def extract_pdf_content(self,json):
        json=  json.copy()
        json['documents'] = []
        circFilename=json['circFilename']
        folder = f'{self.folder}/pdfs' if circFilename.endswith(".pdf") else f'{self.folder}/zips'
        
        if circFilename.endswith(".pdf"):
            file = Path(folder)/circFilename
            if not file.exists():
                json = {id:uuid.uuid4().hex,**json}
                return  json
            
            text=self.extract_text_and_tables(file,json)
            json["documents"].append({file.name:text})
            json.update(id=uuid.uuid4().hex)
    
        
        elif circFilename.endswith(".zip"):
            file = Path(folder)/circFilename
            if not file.exists():
                json = {id:uuid.uuid4().hex,**json}
                return  json
            
            text=self.extractZipContent(json,file)
            json["documents"].append(text)
            json.update(id=uuid.uuid4().hex)

        else:
            json.update(id=uuid.uuid4().hex)

        del json['circFilename']
        return json

    def save(self,circulars):
        with open(f"{self.folder}/final_processed_circulars.json","w") as f:
            json.dump(circulars,f,indent=2)
    
    def get_and_process(self):
        pool = ThreadPoolExecutor(max_workers=3)
        zip_folder = Path(f"{self.folder}/zips")
        circulars = self.get_all_circulars()
        logger.info(f"Fetched all {len(circulars)} circulars")
        self.download_circulars(circulars)
        self.retry(circulars)
        finalExtractedContent = self.map_progress(pool,circulars,self.extract_pdf_content,"Extracting PDF content...")

        logger.info("Extracted text from PDF's")
        self.save(finalExtractedContent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--save_path', default='./data')
    parser.add_argument('--start', default='01-09-2025')
    args = parser.parse_args()

    obj = CircularsFetchProcess(start_date=args.start,folder=args.save_path)
    obj.get_and_process()