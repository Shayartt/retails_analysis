// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("DataIngestion")
  .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC Reading Env Variables to access s3, since I'm using the community edition right now I can't use secretAPI2.0 (can't generate personal token) instead I'll use env variables in my compute instance.

// COMMAND ----------

val accessKey = sys.env("ACCESS_KEY_ID")
val secretKey = sys.env("SECRET_ACCESS_KEY")

// COMMAND ----------

// MAGIC %md
// MAGIC Setup Spark env

// COMMAND ----------

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)

// Specify S3 bucket Path : 
val s3BucketPath = "s3://labdataset/raw/supermarket_sales - Sheet1.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC Read the CSV file : 

// COMMAND ----------

val df = spark.read
  .format("csv")
  .option("header", "true") 
  .load(s3BucketPath)

// COMMAND ----------

display(df.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC Change Column Names : 

// COMMAND ----------

// Specify Columns to lower case and replace space by _
def formatColumnName(name: String): String = {
  name.replace(" ", "_").replace("%","").toLowerCase
}

// Generate a mapping of old column names to new column names
val renamedColumns = df.columns.map(colName => (colName, formatColumnName(colName))).toMap

// Rename columns in the DataFrame
val dfBronze = renamedColumns.foldLeft(df) {
  case (tempDF, (oldName, newName)) => tempDF.withColumnRenamed(oldName, newName)
}


// COMMAND ----------

display(dfBronze.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC Save table into Databricks env

// COMMAND ----------

// MAGIC %sql
// MAGIC Create DATABASE IF NOT EXISTS bronze_retails
// MAGIC comment "Retails Bronze Database"

// COMMAND ----------

dfBronze.write.mode("overwrite").option("mergeSchema","true").saveAsTable("bronze_retails.sales")

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select count(*) from bronze_retails.sales

// COMMAND ----------


