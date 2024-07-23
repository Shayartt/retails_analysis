// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("PreProcessing")
  .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Bronze Data for PreProcessing

// COMMAND ----------

var dfBronze = spark.table("bronze_retails.sales")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Formatting Types : 

// COMMAND ----------

dfBronze = dfBronze.withColumn("date", to_date(col("date"), "M/d/yyyy"))
.withColumn("unit_price", col("unit_price").cast("float"))
.withColumn("quantity", col("quantity").cast("int"))
.withColumn("tax_5", col("tax_5").cast("float"))
.withColumn("total", col("total").cast("float"))
.withColumn("cogs", col("cogs").cast("float"))
.withColumn("gross_income", col("gross_income").cast("float"))
.withColumn("rating", col("rating").cast("float"))

// COMMAND ----------

// MAGIC %md
// MAGIC Show Data

// COMMAND ----------

display(dfBronze.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Feature Engineering: Wil be adding some columns to be used in our analysis later.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Extract hour from time to be used in analytics: 

// COMMAND ----------

var dfSilver = dfBronze.withColumn("hour", hour(col("time")))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Categorize product based on rating :  < 4.9 : bad, between 5 and 7 : good, > 7 very good

// COMMAND ----------

// 
dfSilver = dfSilver.withColumn("product_quality", 
when(col("rating") < 4.9, "poor")
.when(col("rating") < 7, "good")
.otherwise("very good")
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Generate column to check if there is anything else than taxe included in the gross

// COMMAND ----------

dfSilver = dfSilver.withColumn("is_only_tax",
when(col("tax_5") === col("gross_income"), true)
otherwise(false)
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Check on Tax if it's calculated correctly : 

// COMMAND ----------

dfSilver = dfSilver.withColumn("Calculated_tax",
round((col("unit_price") * col("quantity") * 5) / 100, 4)
)
.withColumn("is_tax_correct",
when((round(col("tax_5"),4) - col("Calculated_tax")) <= 0.00001, true) // Rounding issue
otherwise(false)
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Normalize the sales amount using formula : (amount - min) / (max - min) 

// COMMAND ----------

// MAGIC %md
// MAGIC Note : Here we can do it easily using SQL or Dataframe API, but for learning purpose I'll do it using UDFs

// COMMAND ----------

val normalizeSales = udf((amount: Float, min: Float, max: Float) => (amount - min) / (max - min))
val minAmount = dfSilver.agg(min("total")).first().getFloat(0)
val maxAmount = dfSilver.agg(max("total")).first().getFloat(0)
dfSilver = dfSilver.withColumn("normalized_amount", normalizeSales($"total", lit(minAmount), lit(maxAmount)))


// COMMAND ----------

display(dfSilver.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Save Output :

// COMMAND ----------

// MAGIC %sql
// MAGIC Create DATABASE IF NOT EXISTS silver_retails
// MAGIC comment "Retails Silver Database"

// COMMAND ----------

dfSilver.write
  .format("delta") 
  .option("mergeSchema", "true")
  .mode("overwrite") 
  .partitionBy("date")  
  .saveAsTable("silver_retails.sales")

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from silver_retails.sales limit 10

// COMMAND ----------


