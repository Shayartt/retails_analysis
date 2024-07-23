// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("Analysis")
  .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for Analysis

// COMMAND ----------

var dfSilver = spark.table("silver_retails.sales")

// COMMAND ----------

display(dfSilver.limit(10))

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC CREATE DATABASE IF NOT EXISTS gold_retails
// MAGIC comment "Final golden db"

// COMMAND ----------

// MAGIC %md
// MAGIC # Aggregations : 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Aggregation per customer type: 

// COMMAND ----------

var salesPerCustomer_dim = dfSilver.groupBy("customer_type", "gender").agg(
  round(sum("total"), 2).alias("total_sales"),
  round(sum("gross_income"), 2).alias("total_gross_income"),
  round(avg("normalized_amount"), 2).alias("average_normalized_amount"),
  sum("quantity").alias("total_number_of_orders")
)
salesPerCustomer_dim = salesPerCustomer_dim.orderBy(desc("total_sales"))

salesPerCustomer_dim.write
  .format("delta") 
  .mode("overwrite") 
  .saveAsTable("gold_retails.statistics_per_customer_gender_dim")

display(salesPerCustomer_dim.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Aggregation per city, product line and payment type:

// COMMAND ----------

var salesPerCityPaymentLine_dim = dfSilver.groupBy("city", "payment", "product_line").agg(
  round(sum("total"),2).alias("total_sales"),
  sum("quantity").alias("total_number_of_orders"),
  round(avg("rating"),2).alias("average_rating"),
  max("rating").alias("max_rating"),
  min("rating").alias("min_rating")
)


salesPerCityPaymentLine_dim.write
  .format("delta") 
  .option("mergeSchema", "true")
  .mode("overwrite") 
  .saveAsTable("gold_retails.statistics_per_city_payment_line_dim")

display(salesPerCityPaymentLine_dim.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Aggregation per product type, product line and gender:

// COMMAND ----------

var salesPerLineTypeGender_dim = dfSilver.groupBy("product_line","product_quality","gender").agg(
  round(sum("total"), 2).alias("total_sales"),
  sum("quantity").alias("total_number_of_orders"),
  round(avg("rating"), 2).alias("average_rating")
)
salesPerLineTypeGender_dim = salesPerLineTypeGender_dim.orderBy(desc("total_number_of_orders"), desc("total_sales"))

salesPerCityPaymentLine_dim.write
  .format("delta") 
  .option("mergeSchema", "true")
  .mode("overwrite") 
  .saveAsTable("gold_retails.statistics_per_line_quality_gender_dim")

display(salesPerLineTypeGender_dim.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Time-based Analysis : Aggregation count of sales per time per line : 

// COMMAND ----------

var salesPerTimeLine = dfSilver.groupBy("hour", "product_line").agg(
  round(sum("total"), 2).alias("total_sales"),
  sum("quantity").alias("total_number_of_orders"),
  round(avg("rating"), 2).alias("average_rating"),
  min("unit_price").alias("cheaper_product"),
  max("unit_price").alias("expensive_product")
)

salesPerTimeLine = salesPerTimeLine.orderBy(desc("total_number_of_orders"))

display(salesPerTimeLine.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Correlation between count of order and product type & rating : 

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val indexer = new StringIndexer()
  .setInputCol("product_quality")
  .setOutputCol("product_quality_index")
  .fit(dfSilver)

val indexedDF = indexer.transform(dfSilver)

val aggregatedData = indexedDF
  .groupBy("product_quality_index")
  .agg(
    sum("quantity").alias("total_sales_count")
  )

val correlation = aggregatedData.stat.corr("total_sales_count", "product_quality_index")
println(s"Correlation between total sales count and product quality: $correlation")

// COMMAND ----------

// MAGIC %md
// MAGIC Very low Correlation means the the number of sales does not depend on product quality, and this is because our dataset is not big enough to analyze this pattern.

// COMMAND ----------


