import findspark
location = findspark.find() 
findspark.init(location, edit_rc=True)
from pyspark.sql import SparkSession 

class Spark:    
    def __init__(self):
        self.spark = (SparkSession
                .builder
                .config("spark.jars", 
                        "/opt/spark-3.2.0-bin-hadoop3.2/jars/postgresql-42.5.0.jar")
                .getOrCreate())