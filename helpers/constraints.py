# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def get_constraints(objname, constraintType):
    rules = {}
    df = spark.read.table("demodb.constraintTable")
    df = df.filter('Active = "true"')
    for row in df.filter(col("objectName") == objname).collect():
        rules[row['constraintName']] = row['constraint']

    if constraintType == "validdata":
        constraints = rules
    if constraintType == "baddata":
        constraints = "NOT ({0})".format(" AND ".join(rules.values()))
    return constraints
