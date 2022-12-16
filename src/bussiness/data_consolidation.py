from src.dao import DAO
from pyspark.sql import functions as f

def business_loading(**kwargs):
    dao = DAO()
    consolidated_data = dao.load(kwargs['table'])

    consolidated_data = (consolidated_data.groupBy(f.col("age"), f.col("california_region"))
                    .agg(f.sum(f.col("population")).alias("sum_population"), 
                        f.mean("median_house_value").alias("m_median_house_value"))
                    .orderBy(f.col("m_median_house_value"))
                    .withColumn("current_date", f.current_date()))

    dao.save(consolidated_data, "ca_consolidated_data", "overwrite", "current_date")

    return "Data was fully loaded at business layer"


    

    