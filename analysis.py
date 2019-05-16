from pyspark import SparkContext
from numpy import array
from pyspark.sql.types import Row
# from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from decimal import Decimal
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

if __name__ == "__main__":
    
    df = sqlContext.read.csv("arn:aws:s3:::irs-form-990.csv")
    df.show()