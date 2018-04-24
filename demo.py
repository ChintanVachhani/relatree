import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--jars graphframes-release-0-5-0-assembly-0.5.0-spark2.1.jar pyspark-shell')
from pyspark.sql import SparkSession
from graphframes import *
import pickle

spark = SparkSession \
    .builder \
    .appName("RelaTree") \
    .getOrCreate()

# group_members_data = spark.read.csv("group_members.csv", header="true").drop("update_time", "is_admin")
# group_channel_data = spark.read.csv("group_channel.csv", header="true")

# Vertex DataFrame
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
], ["id", "name", "age"])
# Edge DataFrame
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)
g.vertices.show()
g.edges.show()