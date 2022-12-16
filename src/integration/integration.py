from src.dao import DAO
from pyspark.sql import functions as f



def integration_loading(**kargs):
    dao = DAO()
    df = dao.load(kargs['table'])
    stddev_df = (df.groupBy("longitude", "latitude")
                .agg(f.mean("housing_median_age").alias("1"), 
                    f.mean("median_income").alias("2"), 
                    f.mean("median_house_value").alias("3"))
                .agg(f.stddev("1").alias("stddev_median_age"),
                    f.stddev("2").alias("stddev_income"),
                    f.stddev("3").alias("stddev_house_value"))
                .withColumn("current_data", f.current_date()))

    print("Writing table containing standartd deviation according to the data sources...")

    dao.save(stddev_df, "stddev_table", save_mode="overwrite", col_part="current_date")

    df_hma_cat = (df.withColumn("hma_cat", 
                    f.when((f.col("housing_median_age").cast('integer') < 18),
                        "de_0_ate_18")
                    .when((f.col("housing_median_age").cast('integer') >= 18) &
                            (f.col("housing_median_age").cast('integer') < 29),
                            "ate_29")
                    .when((f.col("housing_median_age").cast('integer') >= 29) &
                            (f.col("housing_median_age").cast('integer') < 37),
                            "ate_37")
                    .when((f.col("housing_median_age").cast('integer') >= 37),
                            "acima_37"))
                .withColumn("c_ns", 
                    f.when(f.col("longitude").cast('string') > -119, 
                            "SUL")
                    .otherwise("Norte"))
                .select(f.col("hma_cat").cast("string").alias("age"),
                        f.col("c_ns").cast("string").alias("california_region"),
                        f.col("total_rooms").cast("double"),
                        f.col("total_bedrooms").cast("double"),
                        f.col("population").cast("double"),
                        f.col("households").cast("double"),
                        f.col("median_house_value").cast("double"),
                        f.current_date().alias("current_date")))


    dao.save(df_hma_cat, "californians_consolidated_data", save_mode="overwrite", col_part="current_date")




