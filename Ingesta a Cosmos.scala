// Databricks notebook source
spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

// COMMAND ----------

Thread.currentThread.getContextClassLoader.getResource("com/azure/cosmos/")

// COMMAND ----------

import com.azure.cosmos.spark._

import com.azure.cosmos.spark.CosmosConfig

import com.azure.cosmos.spark.CosmosAccountConfig

// COMMAND ----------

var strCosmosDBUriConfigValue = "https://cosmoscursoudemy.documents.azure.com:443/"

var strCosmosDBKeyConfigValue = "znWkRGK44bZjW5YO10pbauLIG4VYqTRsUZuXsAD20Z6YREVoHThltbrf6bgkXctXNiLxvSQXTTqYACDbXAuJSA=="

var strCosmosDBDatabaseName = "circuits"

var strCosmosDBCollectionName = "circuits"

// var strCosmosDBRegionsConfigValue = 

 

val volcanoDBConfig = Map("spark.cosmos.accountEndpoint" -> strCosmosDBUriConfigValue,

       "spark.cosmos.accountKey" -> strCosmosDBKeyConfigValue,

        "spark.cosmos.database" -> strCosmosDBDatabaseName,

//         "spark.cosmos.preferredRegions" -> strCosmosDBRegionsConfigValue ,

        "spark.cosmos.container" -> strCosmosDBCollectionName,

       "spark.cosmos.read.customQuery" -> "SELECT * FROM c"

   ) 

// COMMAND ----------

val dfCircuits = spark.read.format("delta").load("/mnt/storageformula1dl/processed/circuits")

// COMMAND ----------



// COMMAND ----------

val cosmosCfg = Map("spark.cosmos.accountEndpoint" -> "https://cosmoscursoudemy.documents.azure.com:443/",
       "spark.cosmos.accountKey" -> "yelFTWZtIAT3jXEsBvmk1OY484hDtmQr16xFp5gASPW3ezNDBjyQzapW3yBEaBWIcugIgaGsyeEGACDbNDlang==",
        "spark.cosmos.database" -> "circuits",
        "spark.cosmos.container" -> "circuits"
   ) 

// COMMAND ----------

dfCircuits
.write
.format("cosmos.oltp")//indica el formato tipo de libreria tipo cosmos 
.options(cosmosCfg)
.mode("APPEND")
.save()
//Command took 34.13 minutes -- by acaceresh_2089@hotmail.com at 28/10/2022, 18:35:09 on Cluster_20221028

// COMMAND ----------

val cosmosEndpoint = "https://cosmoscursoudemy.documents.azure.com:443/"
val cosmosMasterKey = "znWkRGK44bZjW5YO10pbauLIG4VYqTRsUZuXsAD20Z6YREVoHThltbrf6bgkXctXNiLxvSQXTTqYACDbXAuJSA=="
val cosmosDatabaseName = "circuits"
val cosmosContainerName = "circuits"

val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)

// COMMAND ----------

display(dfCircuits)

// COMMAND ----------

val df = dfCircuits.toJSON

// COMMAND ----------

df.write
   .format("cosmos.oltp")
   .option("inferSchema",true)
   .options(cfg)
   .mode("APPEND")
   .save()

// COMMAND ----------

spark.createDataFrame(Seq(("cat-alive", "Schrodinger cat", 2, true), ("cat-dead", "Schrodinger cat", 2, false)))
  .toDF("id","name","age","isAlive")
   .write
   .format("cosmos.oltp")
   .options(cfg)
   .mode("APPEND")
   .save()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read a json from fileStore

// COMMAND ----------

val df_population = spark.read.format("json").option("multiline","true").load("/FileStore/tables/population_data.json")

// COMMAND ----------

df_population.printSchema()

// COMMAND ----------

display(df_population)

// COMMAND ----------

val cosmosEndpoint = "https://cosmoscursoudemy.documents.azure.com:443/"
val cosmosMasterKey = "znWkRGK44bZjW5YO10pbauLIG4VYqTRsUZuXsAD20Z6YREVoHThltbrf6bgkXctXNiLxvSQXTTqYACDbXAuJSA=="
val cosmosDatabaseName = "database-v3"
val cosmosContainerName = "population"

val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)

// COMMAND ----------


