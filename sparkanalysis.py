# -*- coding: utf-8 -*-
"""
Created on Sun Apr 28 12:04:18 2019

@author: Tom
"""

from pyspark import SparkContext
import pyspark
import pandas as pd

sc = SparkContext()

spark = pyspark.sql.SparkSession.builder.master("local").appName("sal").getOrCreate()

#pdf = pd.read_csv('names_list.csv')

spdf = (spark.read.format("csv").options(header="true", inferSchema="true").load("names_list.csv"))

#spdf = spark.createDataFrame(pdf)

#pzips = pd.read_csv('zipcodes.csv')

#spzips = spark.createDataFrame(pzips)

spzips = (spark.read.format("csv").options(header="true", inferSchema="true").load("zipcodes.csv"))

#print('test')

spdf.createOrReplaceTempView("names")
spzips.createOrReplaceTempView("zips")

results = spark.sql("SELECT n.*, z.flat, z.slat, z2.Lat, z.flong, z.slong, z2.Long, \
                    n2.Name as matchName, n2.Comp as matchComp, n2.Org as matchOrg, \
                    n2.Zip as matchZip, n2.ReturnID as matchReturnID \
                    FROM names n inner join zips z on n.Zip = z.Zipcode \
                 inner join zips z2 on z2.Lat between z.flat and z.slat and \
                 z2.Long between z.flong and z.slong \
                 inner join names n2 on z2.Zipcode = n2.Zip and n.ReturnID != n2.ReturnID \
                 where levenshtein(n.Name, n2.Name) < 4")

results.write.csv('sparkresults.csv')

#rpnds = results.toPandas()

#rpnds.to_csv('results.csv')