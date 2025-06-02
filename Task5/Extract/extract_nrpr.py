import gzip
import json
import ijson
from decimal import Decimal 
import os

def extract_import(input, logger):
    logger.info("Starting extract process")

    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    output_path = os.path.join(os.getcwd(), 'output_files')
    os.makedirs(output_path, exist_ok=True)
    nrpr_file = os.path.join(output_path, 'nrpr.json')

    for file in os.listdir(input):
        file_path = os.path.join(input, file)
        if file.endswith('.json.gz'):
            try:
                with gzip.open(file_path, 'rt') as f :
                    with open(nrpr_file, 'w') as nrpr:
                        for item in ijson.items(f, ''):
                            nrpr.write(json.dumps(item,default=con_decimal) + '\n')
            except Exception as e:
                logger.info(e)

    logger.info("File extracted successfully")
    return nrpr_file