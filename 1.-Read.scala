// Databricks notebook source
val cosmosEndpoint = "https://cosmosaccountbluetab.documents.azure.com:443/"
val cosmosMasterKey = "ZB56hMXTOwExMCJ3JS0mUPdv24tiy2lfE5VEgGcZ67diySyyYjTjXcEFK3La23M81tzYx3OaDsioACDbjI1Fkg=="
val cosmosDatabaseName = "database-v2"
val cosmosContainerName = "product"

val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read container from cosmosdb

// COMMAND ----------

val product = spark.read.format("cosmos.oltp").options(cfg).load()

// COMMAND ----------

display(product)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read data from ADLS and Write to Cosmos

// COMMAND ----------

val df_results = spark.read.format("delta").load("/mnt/storageformula1dl/processed/results")

// COMMAND ----------

display(df_results)

// COMMAND ----------

df_results.printSchema

// COMMAND ----------

val df_result_group=df_results.groupBy("race_id").sum("points")

// COMMAND ----------

val df_drop_null=df_results.na.drop("all")

// COMMAND ----------

display(df_drop_null)

// COMMAND ----------

val df_json=df_results.toJSON

// COMMAND ----------

display(df_json)

// COMMAND ----------

val cosmosEndpoint = "https://cosmosaccountbluetab.documents.azure.com:443/"
val cosmosMasterKey = "ZB56hMXTOwExMCJ3JS0mUPdv24tiy2lfE5VEgGcZ67diySyyYjTjXcEFK3La23M81tzYx3OaDsioACDbjI1Fkg=="
val cosmosDatabaseName = "database-v3"
val cosmosContainerName = "results"

val cfg2 = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)

// COMMAND ----------

val results_raw_df = spark.read.option("inferSchema","true")
.json("/mnt/storageformula1dl/raw/2021-03-28/results.json")

// COMMAND ----------

df_result_group.write.format("cosmos.oltp").options(cfg2).mode("APPEND").save()

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

val cfg3 = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)

// COMMAND ----------

df_population.write.format("cosmos.oltp").options(cfg3).mode("APPEND").save()
