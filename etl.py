import yaml 
from Extract import extract
#trl
class ETL:
    def __init__(self,logger):
        with open("Task5.yml", 'r') as file:
            Task5 = yaml.safe_load(file)
            
        self.logger = logger
        self.logger.info("Initializing ETL") 

        self.core = Task5['SPARK']['EXECUTOR']
        self.cores = Task5['SPARK']['EXECUTOR']['CORES']
        self.instances = Task5['SPARK']['EXECUTOR']['INSTANCES']
        self.memory = Task5['SPARK']['EXECUTOR']['MEMORY']
        self.memories = Task5['SPARK']['DRIVER']['MEMORY']

        self.dbname = Task5['POSTGRES']['DATABASE']
        self.host = Task5['POSTGRES']['HOST']                                                                                         
        self.port = Task5['POSTGRES']['PORT']
        self.user = Task5['POSTGRES']['USER']
        self.password = Task5['POSTGRES']['PASSWORD']
        
    
    def execute(self,folder):
       
        self.logger.info("Extract")
        extract.extract_nr_pr(folder)