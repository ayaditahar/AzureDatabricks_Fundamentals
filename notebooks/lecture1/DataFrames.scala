// Databricks notebook source
// MAGIC %fs head /mnt/custommount/fakefriends.csv

// COMMAND ----------

spark

// COMMAND ----------

import spark.implicits._

case class Person(id:Int, name:String, age:Int, friends:Int)

// COMMAND ----------

val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/mnt/custommount/fakefriends.csv")
      .as[Person]

// COMMAND ----------

people.show()

// COMMAND ----------

// There are lots of other ways to make a DataFrame.
// For example, spark.read.json("json file path")
// or sqlContext.table("Hive table name")

println("Here is our inferred schema:")
people.printSchema()


// COMMAND ----------

people.show()

// COMMAND ----------

println("Let's select the name column:")
people.select("name").show()

// COMMAND ----------

println("Filter out anyone over 21:")
people.filter(people("age") < 21).show()

// COMMAND ----------

println("Group by age:")
people.groupBy("age").count().show()

// COMMAND ----------

println("Make everyone 10 years older:")
people.select(people("name"), people("age") + 10).show()

// COMMAND ----------

println("show result as dataframe table")
display(people.select(people("name"), people("age") + 10))

// COMMAND ----------

people.createOrReplaceTempView("people")

// COMMAND ----------

val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
val results = teenagers.collect()
results.foreach(println)

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC SELECT * FROM people WHERE age >= 13 AND age <= 19
// MAGIC -- using sql syntax(the result will be showed as table)

// COMMAND ----------

// MAGIC %sql
// MAGIC select age, count(age) from people group by age

// COMMAND ----------

// push result into azure blobstorage
val result = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
result.write.
option("header","true").
format("com.databricks.spark.csv").
save("/mnt/custommount/teangers_output.csv")

// COMMAND ----------

// ***********   md using databses  ************************
%sql
create database if not exists databrickstables

// COMMAND ----------

import spark.implicits._

case class People(country:String, name:String, zip_code:Int)
val people2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/FileStore/tables/createdviadatabricksdb/*csv")
      .as[People]

// COMMAND ----------

people2.show()

// COMMAND ----------

people2.createOrReplaceTempView("people2")

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT * FROM people2 WHERE country = 'usa'

// COMMAND ----------

val result = spark.sql("SELECT * FROM people2 WHERE country = 'usa'")
result.write.
option("header","true").
format("com.databricks.spark.csv").
saveAsTable("databrickstables.savedasdatastore")

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

/FileStore/tables/createdviadatabricksdb/people.csv

// COMMAND ----------



// COMMAND ----------

