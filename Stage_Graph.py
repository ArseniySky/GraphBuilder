# Databricks notebook source
spark.catalog.clearCache()

# COMMAND ----------

import sys
import datetime
import os

from pyspark import SparkConf
from pyspark import SparkContext 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

ADLSStoreName = 'ecadldev'
#adl://ecadldev.azuredatalakestore.net
#Leveraging Azure KeyVault backed DataBricks Secret Scope for authentication serviceprincipalid
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", dbutils.secrets.get('ssedataecdev-secret-scope','ecprodlakeserviceprincipalid')) 
spark.conf.set("dfs.adls.oauth2.credential", dbutils.secrets.get('ssedataecdev-secret-scope','spnkey'))
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dfDept = spark.read.csv("adl://ecadldev.azuredatalakestore.net/dev/raw/Dept/Dept.csv",header=True)
dfEmployee = spark.read.csv("adl://ecadldev.azuredatalakestore.net/dev/raw/Employee/Employee.csv",header=True)

# COMMAND ----------

#display(dfEmployee)
#display(dfDept)

# COMMAND ----------

dfEmployee.createOrReplaceTempView("dfEmployee")
dfDept.createOrReplaceTempView("dfDept")

def CleanUpPartition (DirPath,TargetFileName):
  print(DirPath+"/_SUCCESS")
  rpaths = list(dbutils.fs.ls(DirPath))
  for i in range (0,len(rpaths)):
    if ("_SUCCESS" in rpaths[i].path):
      dbutils.fs.rm(rpaths[i].path)
    if (("_started" in rpaths[i].path) or ("_committed" in rpaths[i].path)):
      dbutils.fs.rm(rpaths[i].path)
    if ((".csv" in rpaths[i].path) or (".parquet" in rpaths[i].path)):
      dbutils.fs.mv(rpaths[i].path,DirPath+"/"+TargetFileName)

# COMMAND ----------

# DBTITLE 1,Model 1  - Where Department is modeled as Vertex
Employee_V = spark.sql("SELECT EmpId as id,EmpName,Age,Salary,'Employee' AS entity FROM dfEmployee")
Dept_V = spark.sql("SELECT DeptId as id,DeptDesc, 'Department' AS entity FROM dfDept")
Employee_E = spark.sql("SELECT EmpId AS src, ManagerId AS dst, 'ReportsTo' AS relationship FROM dfEmployee WHERE ManagerId IS NOT NULL")
Dept_E = spark.sql("SELECT E.EmpId AS src,D.DeptId AS dst,'BelongsTo' AS relationship FROM dfEmployee E INNER JOIN dfDept D ON E.DeptId = D.DeptId")

Employee_V.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V","Employee_V.csv")

Dept_V.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_V",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_V","Department_V.csv")

Employee_E.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E","Employee_E.csv")

Dept_E.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_E",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_E","Department_E.csv")

dfMeta = spark.sql("""
SELECT * FROM (
SELECT 1 AS RID,"Vertex" AS Type, "Employee" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V/Employee_V.csv" AS StageDataPath
UNION ALL
SELECT 2 AS RID, "Vertex" AS Type, "Department" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_V/Department_V.csv" AS StageDataPath
UNION ALL
SELECT 1 AS RID,"Edge" AS Type, "Employee" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E/Employee_E.csv" AS StageDataPath
UNION ALL
SELECT 2 AS RID,"Edge" AS Type, "Department" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Department_E/Department_E.csv" AS StageDataPath
) A """)

dfMeta.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/MetaData",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/MetaData","MetaData.csv")

# COMMAND ----------

# DBTITLE 1,Model 2 - Where Department is modeled as Property
Employee_V = spark.sql("SELECT E.EmpId as id,E.EmpName,E.Age,E.Salary,D.DeptDesc AS Department,'Employee' AS entity FROM dfEmployee E INNER JOIN dfDept D ON E.DeptId = D.DeptId")
Employee_E = spark.sql("SELECT EmpId AS src, ManagerId AS dst, 'ReportsTo' AS relationship FROM dfEmployee WHERE ManagerId IS NOT NULL")

Employee_V.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V","Employee_V.csv")

Employee_E.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E","Employee_E.csv")


dfMeta = spark.sql("""
SELECT * FROM (
SELECT 1 AS RID,"Vertex" AS Type, "Employee" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_V/Employee_V.csv" AS StageDataPath
UNION ALL
SELECT 1 AS RID,"Edge" AS Type, "Employee" AS Label, "adl://ecadldev.azuredatalakestore.net/dev/Staging/Employee_E/Employee_E.csv" AS StageDataPath
) A """)

dfMeta.repartition(1).write.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/MetaData",header='true',sep=',',mode="overwrite")
CleanUpPartition("adl://ecadldev.azuredatalakestore.net/dev/Staging/MetaData","MetaData.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Model 1
# MAGIC --SELECT EmpId as id,EmpName,Age,Salary,"Employee" AS Entity FROM dfEmployee
# MAGIC --SELECT DeptId as id,DeptDesc, "Department" AS Entity FROM dfDept
# MAGIC --SELECT EmpId AS Src, ManagerId AS Dst, "ReportsTo" AS Relationship FROM dfEmployee WHERE ManagerId IS NOT NULL
# MAGIC --SELECT E.EmpId AS Src,D.DeptId AS Dest,"BelongsTo" AS Relationship FROM dfEmployee E INNER JOIN dfDept D ON E.DeptId = D.DeptId
# MAGIC 
# MAGIC 
# MAGIC --Vs Model 2
# MAGIC 
# MAGIC --SELECT EmpId,EmpName,Age,Salary,DeptId, "Employee" AS Entity FROM dfEmployee
# MAGIC --SELECT EmpId AS Src, ManagerId AS Dst, "ReportsTo" AS Relationship FROM dfEmployee WHERE ManagerId IS NOT NULL
