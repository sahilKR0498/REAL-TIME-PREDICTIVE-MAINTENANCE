// Databricks notebook source
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

val namespaceName = "azuretrial"
val eventHubName = "final"
val sasKeyName = "root"
val sasKey = "ryNdDPw2AaX8BvMYjyAWZMUQV8z+bh1W4SEkKaLuRQg="
val connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool)

def sleep(time: Long): Unit = Thread.sleep(time)

def sendEvent(message: String, delay: Long) = {
  sleep(delay)
  val messageData = EventData.create(message.getBytes("UTF-8"))
  eventHubClient.get().send(messageData)
  System.out.println("Sent event: " + message + "\n")
}

val df = spark.read.format("csv").option("header","true").option("sep", ",").option("inferSchema", "true").load("dbfs:/FileStore/tables/ai4i2020___ai4i2020__test_.csv")
df.collect().foreach { row =>
  sendEvent(row.mkString(" "), 5000)
}

// COMMAND ----------


