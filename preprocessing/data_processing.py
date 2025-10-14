import pyspark 
import pandas as pd 
import numpy as np 
import math   



from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row 




ss=SparkSession.builder.master("local").appName("Spotify Playlist Generator").getOrCreate() 


ss.sparkContext.setCheckpointDir("~/scratch") 



spotify_DF = ss.read.csv("spotify_dataset.csv", header=True, inferSchema=True) 



spotify_DF.printSchema() 


spotify_DF.first() 

