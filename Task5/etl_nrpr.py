import yaml
from Extract import extract_nrpr
from Scrub import scrub_nrpr
from Process import process
from Load import load_nrpr
from pyspark.sql import SparkSession

class ETL:
    def __init__(self):
        with open('new.yml','r') as file:
            sp = yaml.safe_load(file)

        self.cores = sp['SPARK']['EXECUTOR']['CORES']
        self.instances = sp['SPARK']['EXECUTOR']['INSTANCES']
        self.executor_memory = sp['SPARK']['EXECUTOR']['MEMORY']
        self.driver_memory = sp['SPARK']['DRIVER']['MEMORY']

        self.port = sp['POSTGRES']['PORT']
        self.host = sp['POSTGRES']['HOST']
        self.user = sp['POSTGRES']['USER']
        self.database = sp['POSTGRES']['DATABASE']
        self.password = sp['POSTGRES']['PASSWORD']
        
    def execute(self,args,logger):
            nrpr_file= extract_nrpr.extract_import(args.input,logger)

            self.spark = SparkSession.builder.appName("etl").config("spark.driver.memory", self.driver_memory).getOrCreate()

            pr_df,pd_df,net_df,bill_join,bill_df=scrub_nrpr.scrub_import(nrpr_file,args.prov,self,logger)
            rate_path,provider_path = process.process(pr_df,pd_df,net_df,bill_join,logger)
            load_nrpr.load_import(rate_path,provider_path,bill_df,self,logger)