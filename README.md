# Spark AnyBlox

This repository contains the AnyBlox plugin for Spark.

## Building

You need Java 11, Maven 3.9, SBT 1.10, and Scala 2.12. We recommend SKDMan for managing those.

After that simply run `sbt package`. The `.jar` file will be produced in `target/scala-2.12`.

## Installation

The plugin needs to be registered with Spark in `spark-defaults.conf`:

```ini
spark.plugins                                           org.anyblox.spark.AnyBloxPlugin
```

You will need the following Arrow jars to be plugged in as well:

- [`arrow-c-data-18.1.0.jar](https://mvnrepository.com/artifact/org.apache.arrow/arrow-c-data/18.1.0)
- [`arrow-vector-18.1.0.jar](https://mvnrepository.com/artifact/org.apache.arrow/arrow-vector)

You can then run `spark-shell` by passing required packages and jars:

```shell
/opt/spark/bin/spark-shell --packages org.scala-lang:toolkit_2.12:0.1.7 --jars "/anyblox/anyblox-spark_2.12-0.1.0-SNAPSHOT.jar,/arrow/arrow-c-data-18.1.0.jar,/arrow/arrow-vector-18.1.0.jar"
```

## Usage

Open `.any` files as dataframes using standard Spark syntax:

```scala
val df = spark.read.format("anyblox").load("/path/to/data.any")
```

You can use the dataframe like any other Spark df, e.g. create a view and query it with SQL:

```scala
df.createTempView("myview")
spark.sql("SELECT * FROM myview").show
```
