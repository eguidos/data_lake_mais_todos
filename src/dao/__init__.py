from src.utils import Spark

class DAO:
    def __init__(self):
        self.spark = Spark().spark

    def save(self, df, table, save_mode, col_part=None): 
        (df.write
                .format('jdbc')    
                .option("url", 
                            "jdbc:postgresql://localhost:5432/data_driven")
                .option("dbtable", table)
                .option("driver", "org.postgresql.Driver")
                .option("user", "data_driven")
                .option("password", "data_driven")
                .partitionBy(col_part)
                .mode(save_mode).save(table))


    def load(self, table):
        return (self.spark
            .read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/data_driven")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table) \
            .option("user", "data_driven").option("password", "data_driven").load())


    def load_csv_file(self, table, file):
        return (self.spark.read.option("header", "true").option("delimiter", ",").csv(file))


