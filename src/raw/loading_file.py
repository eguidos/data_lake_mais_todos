from src.dao import DAO
from pyspark.sql import functions as f
import requests

def raw_loading(**kargs):
    
    dao = DAO()
    req = requests.get(kargs['url'])
    url_content = req.content
    csv_file = open(kargs['file'], kargs['mode'])
    csv_file.write(url_content)
    csv_file.close()


    df = dao.load_csv_file("california_housing_train", kargs['file'])


    print("Data total processed: ")
    df.count()
    print("Writing in the datase...")

    #This table stores raw data in order to maintain data lineage
    dao.save(df.withColumn("current_date", f.current_date()), kargs['table'], 'overwrite', "current_date")


    return f"Data file was properly downloaded"
