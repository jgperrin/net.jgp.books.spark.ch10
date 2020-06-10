The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 10

Welcome to Spark in Action, chapter 10. This chapter is about ingesting streaming data in Apache Spark. Note that the focus of this chapter is on structured streaming (not discretized streaming).

Also contains examples of the usage of the various **sinks** in Spark's structured streaming.


## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**. This chapter has several labs.

### Lab \#100

Using a UDF using the dataframe API.

The `ReadLinesFromFileStreamApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to create read stream to load (ingest) a text file.
3.	Spark creates write stream and writes the text file data to console

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch10
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch10/src/main/python/lab100_read_stream/
```

3. Execute the following spark-submit command to create a jar file to our this application

 ```
spark-submit readLinesFromFileStreamApp.py
 ```

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch10
```

2. cd net.jgp.books.spark.ch10

3. Package application using sbt command

```
sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch10.lab100_read_stream.ReadLinesFromFileStreamScalaApp.scala target/scala-2.12/SparkInAction2-Chapter10-assembly-1.0.0.jar
```

## Notes
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://facebook.com/sparkinaction/) or in [Manning's live site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
