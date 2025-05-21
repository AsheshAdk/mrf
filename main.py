import sys 
from etl import ETL
import logging
import argparse

def main(): 
    parser = argparse.ArgumentParser(description="ETL pipeline for processing ZIP files containing rate and provider data.")
    parser.add_argument("--folder", help="Path to the Zip file to process")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,filename="logging_file.log")
    logger = logging.getLogger("ETL")
    etl = ETL(logger)
    etl.execute(args.folder)
if __name__ == "__main__":
    main()