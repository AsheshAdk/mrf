from pyspark.sql.functions import expr,concat,hash,when,col,array,concat_ws,lpad,regexp_replace
from pyspark.sql.types import ShortType,LongType,ArrayType,IntegerType,DoubleType

def scrub_import(nrpr_file,prov,etl,logger):
    logger.info("Starting scrub process")

    spark = etl.spark

    nrpr_path = spark.read.json(nrpr_file)
    pro_path = spark.read.parquet(prov)

    nrpr_path.printSchema()

    rate_file = (nrpr_path.selectExpr("*", "explode(in_network) as net").drop("in_network")
        .select("*","net.*").drop("net")
        .selectExpr("*", "explode(negotiated_rates) as rates").drop("negotiated_rates")
        .selectExpr("*", "explode(rates.provider_groups) as id").drop("provider_groups")
        .selectExpr("*", "explode(id.npi) as npi","id.tin.type as tin_type", "id.tin.value as tin").drop("id")   
        .selectExpr("*", "explode(rates.negotiated_prices) as prices").drop("rates")
        .select("*", "prices.*").drop("prices")
    )
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    rate_id = (rate_file.withColumn('tin', regexp_replace(col('tin'), '-', ''))
        .withColumn('provider_group_id',hash(concat("npi","tin"))))
    
    rate_id.select('provider_group_id').distinct().count()
    rate_id.select('npi','tin').distinct().count()

    #Provider
    provider = rate_id.select(
        "provider_group_id",
        "npi",
        "tin_type",
        "tin"
    )

    pr_df = (provider.withColumn("tin_type",when(col("tin_type") == "ein", 1)
                        .when(col("tin_type") == "npi", 2))
                        .withColumn("tin_type", col("tin_type").cast(ShortType())) 
                        .withColumn("tin", col("tin").cast(LongType())))

    #Provider Details
    pd_df = (pro_path.selectExpr("*", "loc.lat as lat", "loc.lon as lon") 
            .withColumn("provider_full_name", concat_ws(" ","provider_first_name", "provider_middle_name","provider_last_name"))
            .withColumn("prv_taxonomy",array("prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code"))
            .withColumn("prv_specialty",array("prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc"))
            
            .drop("loc","prv_fax","prv_type_desc","provider_first_name","provider_last_name","provider_middle_name","provider_name_prefix_text",
                "prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code","prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")
            
            .withColumn("prv_taxonomy",expr("filter(prv_taxonomy, x -> x IS NOT NULL AND x != '')")) 
            .withColumn("prv_specialty",expr("filter(prv_specialty, x -> x IS NOT NULL AND x != '')"))
            
            .withColumn("prv_type_code",
                            when(col("prv_type_code") == "P", 1)
                            .when(col("prv_type_code") == "F", 2))
            .withColumn("prv_type_code", col("prv_type_code").cast(ShortType()))
            .withColumn("lon", col("lon").cast(DoubleType()))
            .withColumn("lat", col("lat").cast(DoubleType()))
            )

    #Network Table
    in_net = rate_id.select(
        "billing_code",
        "billing_code_type",
        "negotiation_arrangement",
        "provider_group_id",
        "billing_class",
        "billing_code_modifier",
        "negotiated_rate",
        "negotiated_type",
        "service_code"
        )

    net_df = (in_net.filter(in_net.billing_code.isNotNull() & (in_net.billing_code != ""))
                .withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
)

    #Billing code
    b_df = spark.read.csv('billing_taxonomy_list.csv', header=True, inferSchema=True)
    bill_df = ((b_df.filter(b_df.billing_code.isNotNull() & (b_df.billing_code != ""))
                .drop('_c4','_c5','_c6')
                .withColumn("billing_code", lpad(b_df["billing_code"], 5, "0")))
                .withColumn("taxonomy_list",array(regexp_replace(col("taxonomy_list"), r"^\{|\}$", "")))
            )
    bill_join = bill_df.select('billing_code','taxonomy_list')
  
    bill_df.printSchema()

    return pr_df,pd_df,net_df,bill_join,bill_df