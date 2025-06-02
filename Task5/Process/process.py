from pyspark.sql.functions import size,array_intersect,col

def process(pr_df,pd_df,net_df,bill_join,logger):

    nr_table = net_df.join(bill_join,on ="billing_code",how= "inner")
    pr_table = pr_df.join(pd_df,on =["npi","tin"],how= "inner")
    npi_table = pr_df.join(pd_df,on ="npi",how= "left_anti")

    pr_table.printSchema()
    nr_table.printSchema()

    nrpr = nr_table.join(pr_table,on="provider_group_id",how="inner")

    specialized_filter = nrpr.filter(size(array_intersect(col("prv_taxonomy"), col("taxonomy_list"))) > 0)
    specialized_filter.show(5)

    nr_table = nr_table.drop('taxonomy_list')
    rate_path = "output_files/nr"
    provider_path = "output_files/pr"
    npi_path = "output_files/left_npi"

    nr_table.write.mode("overwrite").parquet(rate_path)
    pr_table.write.mode("overwrite").parquet(provider_path)
    npi_table.write.mode("overwrite").parquet(npi_path)

    logger.info("File transformed successfully")
    return rate_path,provider_path