// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._

val namespaceName = "azuretrial"
val eventHubName = "final"
val sasKeyName = "root"
val sasKey = "ryNdDPw2AaX8BvMYjyAWZMUQV8z+bh1W4SEkKaLuRQg=" 
val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val customEventhubParameters =
  EventHubsConf(connStr.toString())
  .setMaxEventsPerTrigger(50)

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

val messages =
  incomingStream
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Data", $"body".cast(StringType))
  .select("Data")

//messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

messages.writeStream.format("delta").outputMode("append").option("checkpointLocation","/data/events/_checkpoints/data_file_1").table("i2")

// COMMAND ----------

val messages = spark.readStream.table("e2").select("Data")

// COMMAND ----------

val query = messages.writeStream.outputMode("append").format("console").start()

// COMMAND ----------

// MAGIC %python
// MAGIC rdd2 = spark.sql("""select 
// MAGIC           substring(Data,1,charindex(' ',data)-1) as UDI, 
// MAGIC           substring(data,4,charindex(' ',data)+4) as Product_ID, 
// MAGIC           substring(data,11,charindex(' ',data)-1) as Type, 
// MAGIC           substring(data,14,charindex(' ',data)+0) as Air_temperature,
// MAGIC           substring(data,17,charindex(' ',data)+1) as Process_temperature,
// MAGIC           substring(data,21,charindex(' ',data)+2) as Rotational_speed,
// MAGIC           substring(data,26,charindex(' ',data)+0) as Torque,
// MAGIC           substring(data,29,charindex(' ',data)-1) as TWF,
// MAGIC           substring(data,31,charindex(' ',data)-1) as HDF,
// MAGIC           substring(data,33,charindex(' ',data)-1) as PWF,
// MAGIC           substring(data,35,charindex(' ',data)-1) as OSF,
// MAGIC           substring(data,37,charindex(' ',data)-1) as RNF from i2""").rdd

// COMMAND ----------

// MAGIC %python
// MAGIC df = rdd2.toDF()
// MAGIC df.show(100)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import DecimalType, IntegerType
// MAGIC df1 = df.withColumn("UDI", df["UDI"].cast(DecimalType())).withColumn("Air_temperature", df["Air_temperature"].cast(DecimalType())).withColumn("Process_temperature", df["Process_temperature"].cast(DecimalType())).withColumn("Rotational_speed", df["Rotational_speed"].cast(DecimalType())).withColumn("Torque", df["Torque"].cast(DecimalType())).withColumn("TWF", df["TWF"].cast(IntegerType())).withColumn("HDF", df["HDF"].cast(IntegerType())).withColumn("PWF", df["PWF"].cast(IntegerType())).withColumn("OSF", df["OSF"].cast(IntegerType())).withColumn("RNF", df["RNF"].cast(IntegerType()))

// COMMAND ----------

// MAGIC %python
// MAGIC df1.show(100)

// COMMAND ----------


