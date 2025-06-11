# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
spark = SparkSession.builder.appName("sPARK_TABLE").getOrCreate()
# Sample data
employees = [
    ("A", 90000, "BNG", "IT"),
    ("B", 80000, "HYD", "IT"),
    ("C", 70000, "BNG", "IT"),
    ("D", 100000, "HYD", "IT"),
    ("E", 40000, "HYD", "CS")]
df = spark.createDataFrame(employees,["name","salary","city","department"])
# Maximum salary from each City and Department
result = df.groupBy("city","department").agg(max("salary").alias("max_salary"))
result.show()
