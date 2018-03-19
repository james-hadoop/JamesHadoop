package com.james.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Tinkpad on 2017/2/7.
  */
object SparkSqlDemo extends App {
    System.setProperty("hive.metastore.uris", "thrift://localhost:9083")
    System.setProperty("driver-class-path", "/home/james/install/spark/jars/mysql-connector-java-5.1.38-bin.jar")

    println("SparkSqlDemo start...")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value")
      .config("driver-class-path", "/Users/qjiang/software/mysql-connector-java-5.1.44/mysql-connector-java-5.1.44-bin.jar")
      .getOrCreate()


    // This import is needed to use the $-notation
    import spark.implicits._

    //  val df = spark.read.json("people.json")
    //  df.show()"driver-class-path", "/home/james/install/spark/jars/mysql-connector-java-5.1.38-bin.jar"
    //
    //  // Print the schema in a tree format
    //  df.printSchema()
    //  // root
    //  // |-- age: long (nullable = true)
    //  // |-- name: string (nullable = true)
    //
    //  // Select only the "name" column
    //  df.select("name").show()
    //  // +-------+
    //  // |   name|
    //  // +-------+
    //  // |Michael|
    //  // |   Andy|
    //  // | Justin|
    //  // +-------+
    //
    //  // Select everybody, but increment the age by 1
    //  df.select($"name", $"age" + 1).show()
    //  // +-------+---------+
    //  // |   name|(age + 1)|
    //  // +-------+---------+
    //  // |Michael|     null|
    //  // |   Andy|       31|
    //  // | Justin|       20|
    //  // +-------+---------+
    //
    //  // Select people older than 21.config("spark.some.config.option", "some-value")
    //  df.filter($"age" > 21).show()
    //  // +---+----+
    //  // |age|name|
    //  // +---+----+
    //  // | 30|Andy|
    //  // +---+----+
    //
    //  // Count people by age
    //  df.groupBy("age").count().show()
    //  // +----+-----+
    //  // | age|count|
    //  // +----+-----+.config("spark.some.config.option", "some-value")
    //  // |  19|    1|
    //  // |null|    1|
    //  // |  30|    1|
    //  // +----+-----+
    //
    //  // Register the DataFrame as a SQL temporary view
    //  df.createOrReplaceTempView("people")
    //
    //  val sqlDF = spark.sql("SELECT * FROM people")
    //  sqlDF.show()
    //  // +----+-------+
    //  // | age|   name|
    //  // +----+-------+
    //  // |null|Michael|
    //  // |  30|   Andy|
    //  // |  19| Justin|
    //  // +----+-------+
    //
    //  // Register the DataFrame as a global temporary view
    //  df.createGlobalTempView("people")
    //
    //  // Global temporary view is tied to a system preserved database `global_temp`
    //  spark.sql("SELECT * FROM global_temp.people").show()
    //  // +----+-------+
    //  // | age|   name|
    //  // +----+-------+
    //  // |null|Michael|
    //  // |  30|   Andy|
    //  // |  19| Justin|
    //  // +----+-------+
    //
    //  // Global temporary view is cross-session
    //  spark.newSession().sql("SELECT * FROM global_temp.pe.config("spark.some.config.option", "some-value")ople").show()
    //
    //  // +----+-------+
    //  // | age|   name|
    //  // +----+-------+
    //  // |null|Michael|
    //  // |  30|   Andy|
    //  // |  19| Justin|
    //  // +----+-------+
    //
    //  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    //  // you can use custom classes that implement the Product interface
    //  case class Person(name: String, age: Long)
    //
    //  // Encoders are created for case classes
    //  val caseClassDS = Seq(Person("Andy", 32)).toDS()
    //  caseClassDS.show()
    //  // +----+---+
    //  // |name|age|
    //  // +----+---+
    //  // |Andy| 32|
    //  // +----+---+
    //
    //  // Encoders for most common types are automatically provided by importing spark.implicits._
    //  val primitiveDS = Seq(1, 2, 3).toDS()
    //  primitiveDS.map(_ + 1).collect()
    //  // Returns: Array(2, 3, 4)
    //
    //  // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    //  val path = "people.json"
    //  val peopleDS = spark.read.json(path).as[Person]
    //  peopleDS.show()
    //  // +----+-------+
    //  // | age|   name|
    //  // +----+-------+
    //  // |null|Michael|
    //  // |  30|   Andy|
    //  // |  19| Justin|
    //  // +----+-------+
    //
    //  import org.apache.spark.sql.types._
    //
    //  // Create an RDD of Person objects from a text file, convert it to a Dataframe
    //  val peopleDF2 = spark.sparkContext
    //    .textFile("people.txt")
    //    .map(_.split(",")).config("spark.some.config.option", "some-value")
    //    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    //    .toDF()
    //  // Register the DataFrame as a temporary view
    //  peopleDF2.createOrReplaceTempView("people")
    //
    //  // SQL statements can be run by using the sql methods provided by Spark
    //  val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    //
    //  // The columns of a row in the result can be accessed by field index
    //  teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    //  // +------------+
    //  // |       value|
    //  // +------------+
    //  // |Name: Justin|
    //  // +------------+
    //
    //  // or by field name
    //  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    //  // +------------+
    //  // |       value|
    //  // +------------+
    //  // |Name: Justin|
    //  // +------------+
    //
    //  // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    //  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //  // Primitive types and case classes can be also defined as
    //  // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()
    //
    //  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    //  teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    //  // Array(Map("name" -> "Justin", "age" -> 19))
    //
    //  // Create an RDD
    //  val peopleRDD = spark.sparkContext.textFile("people.txt")
    //
    //  // The schema is encoded in a string
    //  val schemaString = "name age""driver", "com.mysql.jdbc.Driver"
    //
    //  // Generate the schema based on the string of schema
    //  val fields = schemaString.split(" ")
    //    .map(fieldName => StructField(fieldName, StringType, nullable = true))
    //  val schema = StructType(fields)
    //
    //  // Convert records of the RDD (people) to Rows
    //  val rowRDD = peopleRDD
    //    .map(_.split(","))
    //    .map(attributes => Row(attributes(0), attributes(1).trim))
    //
    //  // Apply the schema to the RDD
    //  val peopleDF3 = spark.createDataFrame(row\"RDD, schema)
    //
    //  // Creates a temporary view using the DataFrameCase class
    //  peopleDF3.createOrReplaceTempView("people")
    //
    //  // SQL can be run over a temporary view created using DataFrames
    //  val results = spark.sql("SELECT name FROM people")
    //
    //  // The results of SQL queries are DataFrames and support all the normal RDD operations
    //  // The columns of a row in the result can be accessed by field index or by field name
    //  results.map(attributes => "Name: " + attributes(0)).show()
    //  // +-------------+
    //  // |        value|
    //  // +-------------+
    //  // |Name: Michael|
    //  // |   Name: Andy|
    //  // | Name: Justin|
    //  // +-------------+
    //
    //  /*
    //   * the above result has been successfully printed
    //   */

    val usersDF = spark.read.load("users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    val peopleDF4 = spark.read.format("json").load("people.json")
    peopleDF4.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    val sqlDF2 = spark.sql("SELECT * FROM parquet.`users.parquet`")

    val peopleDF5 = spark.read.json("people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF5.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()


    spark.sparkContext.addJar("/home/james/install/spark/jars/mysql-connector-java-5.1.38-bin.jar")
    //spark.sparkContext.addJar("hdfs://localhost:9000//lib/mysql-connector-java-5.1.38-bin.jar")

    Class.forName("com.mysql.jdbc.Driver")
    //Class.forName("com.mysql.jdbc.Driver")

    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/james")
      .option("dbtable", "james.parquet")
      .option("user", "root")
      .option("password", "root")
      .load()

    //val jdbcDF = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/james", "dbtable" -> "james.parquet", "user" -> "root", "password" -> "root")).load()

    //    val connectionProperties = new Properties()
    //    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    //    connectionProperties.put("user", "username")
    //    connectionProperties.put("password", "password")
    //    val jdbcDF2 = spark.read
    //      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    //
        // Saving data to a JDBC source
        jdbcDF.write
          .format("jdbc")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("url", "jdbc:mysql://localhost:3306/james")
          .option("dbtable", "james.parquet_copy")
          .option("user", "root")
          .option("password", "root")
          .save()
    //
    //    jdbcDF2.write
    //      .jdbc("jdbc:mysql://localhost:3306:james", "james.parquet", connectionProperties)

    println("SparkSqlDemo stop...")
}
