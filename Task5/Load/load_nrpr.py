import psycopg2

def load_import(rate_path,provider_path,bill_df,etl,logger):
    logger.info("Starting load process")
    
    spark = etl.spark
    port = etl.port
    host = etl.host
    user = etl.user
    database = etl.database
    password = etl.password

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    jdbc_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
   
    connection = psycopg2.connect(
        host= host,   
        dbname = database,
        user= user,        
        password=password,
        port= port)
    cursor = connection.cursor()
   
    pr_table = """
    DROP TABLE IF EXISTS pr_table;
    CREATE TABLE pr_table(
        provider_group_id INT,
        npi BIGINT,
        tin_type SMALLINT,
        tin BIGINT,
        prv_city VARCHAR,
        prv_phone VARCHAR,
        prv_state VARCHAR,
        prv_street_1 VARCHAR,
        prv_type_code SMALLINT,
        prv_zip VARCHAR,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        provider_full_name VARCHAR,
        prv_taxonomy TEXT[],
        prv_specialty TEXT[],
        geom GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography)STORED
    );
    """

    cursor.execute(pr_table)
    connection.commit()

    nr_table= """
    DROP TABLE IF EXISTS nr_table;
    CREATE TABLE nr_table(
        billing_code VARCHAR(5),
        billing_code_type VARCHAR,
        negotiation_arrangement VARCHAR,    
        provider_group_id INT,     
        billing_class VARCHAR,
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,    
        negotiated_type VARCHAR,
        service_code INTEGER[]
    );
    """
    cursor.execute(nr_table)
    connection.commit()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS taxonomy;")
    connection.commit()

    bill_table = """
    DROP TABLE IF EXISTS taxonomy.bill_table;
    CREATE TABLE IF NOT EXISTS taxonomy.bill_table(
        billing_code VARCHAR(5),
        billing_code_type VARCHAR(10),
        billing_description VARCHAR,
        taxonomy_list TEXT[]
    );
    """
    cursor.execute(bill_table)
    connection.commit()

    network_data = spark.read.parquet(rate_path)
    provider_data = spark.read.parquet(provider_path)

    provider_data.write.jdbc(url=jdbc_url,table="pr_table",mode="append", properties=jdbc_properties)
    network_data.write.jdbc(url=jdbc_url,table="nr_table",mode="append", properties=jdbc_properties)
    bill_df.write.jdbc(url=jdbc_url,table="taxonomy.bill_table",mode="append", properties=jdbc_properties)

    logger.info("Successfully load to postgresql")
    cursor.close()
    connection.close()