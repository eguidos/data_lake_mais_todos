import json
import pathlib
import findspark
from src.utils import Spark
from pyspark.sql.types import *
findspark.init()
from pyspark.sql import SparkSession 

PATH_PROJECT = str(pathlib.Path(__file__).parent.parent.absolute())
PATH_RESOURCES = f"{PATH_PROJECT}/data/"
