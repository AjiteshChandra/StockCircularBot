from pathlib import Path
from datetime import datetime as dt
from src.logger import setup_logging
import sys
import logging
from src.processCirculars import CircularsFetchProcess
from src.qdrant import QdrantManager
from src.embedding import EmbedContent
import argparse

log_path = Path.cwd() / 'logs'
setup_logging("pipeline", log_dir=log_path,to_console=True,console_filter_keywords=["Failed","successfully","Error",'ready'])
logger = logging.getLogger(__name__)


def get_args():
    parser = argparse.ArgumentParser(description='Run multiple scripts with arguments')
    parser.add_argument('--start', default=dt.today().strftime("%d-%m-%Y"),help='start date for to download circulars')
    parser.add_argument('--save_path', default='./data',help='Folder to save circulars')
    return parser.parse_args()
    


def main():
  
    args = get_args()
    logging.info("Fetching circulars ....")
    circobj = CircularsFetchProcess(start_date = args.start,folder=args.save_path)
    status =circobj.get_and_process()
    if status:
        logging.info("Circulars Saved successfully")
        print()


        qobj = QdrantManager()
        qobj.start_docker_service()
        logging.info("Docker started successfully")
        qobj.start()
        print()

        embdob = EmbedContent(folder=args.save_path)
        embdob.embedData()
        logging.info("Embedded pdf content successfully")
    else:
        print('No new updated circulars or data')
        sys.exit(1)
        logger.info("No new updated circulars or data")



if __name__ == "__main__":
    main()
