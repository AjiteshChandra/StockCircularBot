from nsepython import nsefetch
import io
from datetime import datetime as dt,timedelta
from pathlib import Path
import pdfplumber
import shutil
import pandas as pd
import os
import argparse
import re
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
from collections import defaultdict
import requests
from src.logger import setup_logging
from zipfile import ZipFile
import uuid
import hashlib

log_path = Path.cwd() / 'logs'
# setup_logging("Data_fetch", log_dir=log_path,to_console=True,console_filter_keywords=["Failed","Successfully","Error",'ready'])
logger = logging.getLogger(__name__)

class CircularsFetchProcess:
    def __init__(self,start_date:str|None=None,end_date:str|None=None,folder:str="data/"):
        self.start_date=start_date
        self.corpoStart= start_date
        self.end_date=dt.today().strftime("%d-%m-%Y") if not end_date else end_date
        self.corpoEnd = self.end_date
        self.folder=folder
        self.track = {}

    def convert_to_rfc(self,date):
        
        date_str = date
        formats = ['%B %d, %Y', '%d-%b-%Y']
        # Parse the string into a datetime object
        for fmt in formats:
            try:
                date_obj = dt.strptime(date_str, fmt)
            except ValueError:
                continue
   
        # Convert to RFC 3339 / ISO 8601 format string
        rfc_date_str = date_obj.isoformat()
        
        return  rfc_date_str # Output: 2025-10-25T00:00:00+00:00
    
    def parse_dates(self,circulars:list[dict],columns:list[str]):
        circulars_data = circulars.copy()
        for col in columns:
            for circular in circulars_data:
                circular[col] = self.convert_to_rfc(circular[col])

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
    def deleteCircFolders(self):
        try:
            pdfpath = Path(self.folder)/"pdfs"
            zipPath = Path(self.folder)/"zips"
            if pdfpath.exists():
                shutil.rmtree(pdfpath)
            if zipPath.exists():
                shutil.rmtree(zipPath)
        except Exception as e:
            logger.error(f"Could not delete files from {self.folder}:{e}")

           

    def removeDuplicateCirculars(self,circulars:list[dict]):
        seen = set()
        unique = []
        for d in circulars:
            if d["circFilelink"] not in seen:
                seen.add(d["circFilelink"])
                unique.append(d)
        return unique
    
    def get_all_circulars(self):
        print(f"Circ start:{self.start_date},Circ:{self.end_date}")
        if not self.end_date:
            self.end_date= dt.today().strftime("%d-%m-%Y")

        nsefetch("https://www.nseindia.com")
        url = f'https://www.nseindia.com/api/circulars?fromDate={self.start_date}&toDate={self.end_date}'
        circulars = nsefetch(url)
        print(len(circulars['data']))
        if len(circulars['data']) == 0:
            logger.info("No new circulars to add in db")
            return None
        
        final_circulars = [{k: v for k, v in i.items() if k not in ('circFileSize','circDisplayNo','cirDate','fileExt')} for i in circulars["data"]]
        final_circulars = self.removeDuplicateCirculars(final_circulars)
        final_circulars = self.parse_dates(final_circulars,columns=['cirDisplayDate'])
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
    
    def download_circulars(self,circulars_list:list[dict],desc="Fetching NSE Circulars from .."):
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
            if not (Path(f"{self.folder}/pdfs/{file_name}.pdf").exists() or Path(f"{self.folder}/zips/{file_name}.zip").exists()):
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
                    if Path(name).stem == Path(file).stem:
                        text = self.extract_text_and_tables(pdf_file,json,circ=True)
                        final.update({Path(name).stem+'.pdf': text})
                    else:
                        text = self.extract_text_and_tables(pdf_file,json,circ=False)
                        final.update({Path(name).stem+'.pdf': text})

        return final
    
    def getTables(self,json,page,circ=False):
        """Extract Tables from all the pages in PDF"""
        all_tables=[]
        tables= page.find_tables()
        ## Skipping the circular ref table(since its already present in metadata)
        if circ:
            start = 0 if page.page_number > 1 else 1
        else:
            start = 0
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
    
    def extract_text_and_tables(self,file,json,circ=False):
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

                    pattern = r'\n(?:Sub:|Subject:)\s*-*\s*[^\n]+\n'
                    outside_text  = re.sub(pattern, '\n', outside_text)
                    tables = self.getTables(json,page,circ=circ)
                    page_text = {'page_number':page.page_number,"page_text":outside_text.strip(),'tables':tables}
                    all_page_text.append(page_text)

                
        except Exception as e:
            logger.error(f"Error in extracting content from {file}")
            return None
         
        return all_page_text
    def saveTracking(self,circular_data=None,corpoData=None):
        
        # track['circLastUp']=self.end_date
        if circular_data: 
            self.track['circLastUp']=dt.strptime((circular_data[-1]["cirDisplayDate"]),"%Y-%m-%dT%H:%M:%S").strftime("%d-%m-%Y")
        if corpoData:
            self.track["corpoLastUp"] = dt.strptime((corpoData[-1]['exDate']),"%Y-%m-%dT%H:%M:%S").strftime("%d-%m-%Y")
        

        self.save(self.track,folder="logs/tracking",filename="track_log")

    def load_track(self):
        file_path = f"logs/tracking/track_log.json"

        if os.path.exists(file_path):
            logger.info("Last upserted Log file found")
            with open(file_path) as f:
                self.track=json.load(f)
            
            lastUpCirc = self.track['circLastUp']
            lastUpCorpo =  self.track["corpoLastUp"]
            if dt.strptime(self.start_date,'%d-%m-%Y') <= dt.strptime(lastUpCirc,'%d-%m-%Y'):
                logger.info(f"Circulars data already upserted in db till {lastUpCirc}")
                self.start_date = (dt.strptime(lastUpCirc, '%d-%m-%Y')) +  pd.offsets.BusinessDay(1)
                if (dt.strptime(self.end_date,'%d-%m-%Y') - self.start_date,'%d-%m-%Y')==0:
                    self.start_date = self.end_date = (dt.today()).strftime("%d-%m-%Y")
                else:
                    self.end_date =(self.start_date+timedelta(days=1)).strftime("%d-%m-%Y")

            if dt.strptime(self.corpoStart,'%d-%m-%Y') <= dt.strptime(lastUpCorpo,'%d-%m-%Y'):
                logger.info(f"Corpo actions data already upserted in db till {lastUpCorpo}")
                self.corpoStart=dt.strptime(lastUpCorpo,'%d-%m-%Y')+pd.offsets.BusinessDay(1)
                self.corpoEnd = (self.corpoStart + timedelta(days=10)).strftime("%d-%m-%Y")
                self.corpoStart=self.corpoStart.strftime("%d-%m-%Y")

            return True
        else:
            logger.info("Previous log tracking could not be found")

            #If the latest date is greater than eq start date,then corpoEnd will latest date + 10D
            if dt.strptime(self.start_date,'%d-%m-%Y') < dt.today():
                self.corpoEnd = ( dt.today() + timedelta(days=10)).strftime("%d-%m-%Y")
            
            return False

    def getCorpoData(self):
        nsefetch("https://www.nseindia.com")
        print(f"Corpo start:{self.corpoStart},End:{self.corpoEnd}")
        ca_data_eq = nsefetch(rf"https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date={self.corpoStart}&to_date={self.corpoEnd}")
        ca_data_sme=nsefetch(rf"https://www.nseindia.com/api/corporates-corporateActions?index=sme&from_date={self.corpoStart}&to_date={self.corpoEnd}")
        ca_data_all = ca_data_eq+ca_data_sme
        print(len(ca_data_eq))
        if len(ca_data_eq) ==0:
            logger.info("No new Corporate action data")
            return None
        ca_data_all=[{k:v for k,v in data.items() if k not in ('ind','bcEndDate','bcStartDate','ndStartDate','ndEndDate','isin','caBroadcastDate')} for data in ca_data_all]
        ca_data_all = self.parse_dates(ca_data_all,columns=["exDate","recDate"])
        ca_data_all.sort(key=lambda x:x['exDate'])
        return ca_data_all
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
            
            text=self.extract_text_and_tables(file,json,circ=True)
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

    def save(self,circulars,folder,filename):
        os.makedirs(folder,exist_ok=True)
        with open(f"{folder}/{filename}.json","w") as f:
            json.dump(circulars,f,indent=2)
    
    def get_and_process(self):
        pool = ThreadPoolExecutor(max_workers=3)
        # zip_folder = Path(f"{self.folder}/zips")
        self.load_track()

        circulars = self.get_all_circulars()
        corpo_data = self.getCorpoData()
        if not circulars and not corpo_data:
            logger.info("Latest Data already upserted to DB")
            return None
        
        if  circulars != None:
            logger.info(f"Fetched all {len(circulars)} circulars")
            self.download_circulars(circulars)
            self.retry(circulars)
            finalExtractedContent = self.map_progress(pool,circulars,self.extract_pdf_content,"Extracting PDF content...")
            finalExtractedContent.sort(key=lambda x:x["cirDisplayDate"])
            logger.info("Extracted text from PDF's")

            self.save(circulars=finalExtractedContent,folder=self.folder,filename="final_processed_circulars")
            self.deleteCircFolders()
            logger.info(f"Deleted folders where circulars were saved locally from {self.folder}")
            self.saveTracking(circular_data=finalExtractedContent,corpoData=None)

        
        if  corpo_data != None:
            logger.info(f"Fetched Corporate actions data...")
            self.save( corpo_data,folder=self.folder,filename='corporate_actions_data')
            logger.info(f"Saved Corporate actions data in {self.folder}")
            self.saveTracking(circular_data=None,corpoData=corpo_data)

        return True
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--save_path', default='./data')
    parser.add_argument('--start', default='10-11-2025')
    args = parser.parse_args()

    obj = CircularsFetchProcess(start_date=args.start,folder=args.save_path)
    obj.get_and_process()