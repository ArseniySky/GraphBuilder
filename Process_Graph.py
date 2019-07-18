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
import pandas as pd

ADLSStoreName = 'ecadldev'
#adl://ecadldev.azuredatalakestore.net
#Leveraging Azure KeyVault backed DataBricks Secret Scope for authentication serviceprincipalid
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", dbutils.secrets.get('ssedataecdev-secret-scope','ecprodlakeserviceprincipalid')) 
spark.conf.set("dfs.adls.oauth2.credential", dbutils.secrets.get('ssedataecdev-secret-scope','spnkey'))
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

df = spark.read.csv("adl://ecadldev.azuredatalakestore.net/dev/Staging/MetaData/MetaData.csv",header=True)
dfMetadata = df.toPandas()
dfMetadata=dfMetadata.reset_index(drop=True)

# COMMAND ----------

dfMetadata.RID=dfMetadata.RID.astype(int)
dfMetadata.info()
dfMetadata.columns

# COMMAND ----------

from pyspark.sql.types import StringType
from urllib.parse import quote
def urlencode(value):
  return quote(value, safe="")
udf_urlencode = udf(urlencode, StringType())

def to_cosmosdb_vertices(dfVertices, labelColumn, partitionKey = ""):
  dfVertices = dfVertices.withColumn("id", udf_urlencode("id"))
  columns = ["id", labelColumn]
  if partitionKey:
    columns.append(partitionKey)
  columns.extend(['nvl2({x}, array(named_struct("id", uuid(), "_value", {x})), NULL) AS {x}'.format(x=x) \
                for x in dfVertices.columns if x not in columns])
  return dfVertices.selectExpr(*columns).withColumnRenamed(labelColumn, "label")


from pyspark.sql.functions import concat_ws, col

def to_cosmosdb_edges(g, labelColumn, partitionKey = ""): 
  dfEdges = g.edges  
  if partitionKey:
    dfEdges = dfEdges.alias("e") \
      .join(g.vertices.alias("sv"), col("e.src") == col("sv.id")) \
      .join(g.vertices.alias("dv"), col("e.dst") == col("dv.id")) \
      .selectExpr("e.*", "sv." + partitionKey, "dv." + partitionKey + " AS _sinkPartition")
  dfEdges = dfEdges \
    .withColumn("id", udf_urlencode(concat_ws("_", col("src"), col(labelColumn), col("dst")))) \
    .withColumn("_isEdge", lit(True)) \
    .withColumn("_vertexId", udf_urlencode("src")) \
    .withColumn("_sink", udf_urlencode("dst")) \
    .withColumnRenamed(labelColumn, "label") \
    .drop("src", "dst")
  return dfEdges

# COMMAND ----------

from graphframes import GraphFrame
cosmosEndpoint = dbutils.secrets.get('ssedataecdev-secret-scope','cosmos-endpoint')
cosmosdbkey = dbutils.secrets.get('ssedataecdev-secret-scope','cosmosdbkey')
cosmosDbConfig = {
"Endpoint" :cosmosEndpoint,
"Masterkey" :cosmosdbkey,
"Database" : "graphdb",
"Collection" : "EmployeeModel2", 
"Upsert" : "true"
}
cosmosDbFormat = "com.microsoft.azure.cosmosdb.spark"

UniqueList = sorted(set(list(map(int, dfMetadata.RID.tolist()))))
for i in UniqueList:
  ProcessVertex = dfMetadata.loc[(dfMetadata['RID'] == i) & (dfMetadata['Type'] == 'Vertex')]
  VPath = ProcessVertex.StageDataPath.iloc[0]
  PocessEdge = dfMetadata.loc[(dfMetadata['RID'] == i) & (dfMetadata['Type'] == 'Edge')]
  EPath = PocessEdge.StageDataPath.iloc[0]
  v = spark.read.csv(VPath,header=True)
  e = spark.read.csv(EPath,header=True)
  g = GraphFrame(v,e)
  cosmosDbVertices = to_cosmosdb_vertices(g.vertices, "entity")
  cosmosDbEdges = to_cosmosdb_edges(g, "relationship")
  cosmosDbVertices.write.format(cosmosDbFormat).mode("append").options(**cosmosDbConfig).save()
  cosmosDbEdges.write.format(cosmosDbFormat).mode("append").options(**cosmosDbConfig).save()

# COMMAND ----------


