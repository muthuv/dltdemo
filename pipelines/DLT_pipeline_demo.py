# Databricks notebook source
import sys, os


# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import helpers.constraints as con

# COMMAND ----------

@dlt.view(
    name = "vw_student"
)
@dlt.expect_all_or_drop(con.get_constraints('demodb.student',"validdata"))
def vw_student():
    return(
        spark.sql("""
                  select *, 'src' as srcname from demodb.student
                  """
        )
    )

# COMMAND ----------

@dlt.table(
    name = "student_dlt"
)
def student_dlt():
     return(
        spark.sql("""
                  select * from live.vw_student
                  """
        )
    )
