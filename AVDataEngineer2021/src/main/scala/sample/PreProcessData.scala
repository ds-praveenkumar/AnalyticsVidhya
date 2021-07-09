package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import java.io.{FileNotFoundException, IOException}

object PreProcessData {
  val appName = "PreprocessData"
<<<<<<< HEAD

=======
>>>>>>> 4e2b5915c0e67d573af211e7465b7a578e4175c9
  def main(args: Array[String]): Unit = {
    val userTablePath  = args(0).trim()
    val visitorLogDataPath = args(1).trim()
    val today = args(2).trim()
    val outputPath = args(3) // "src/main/resources/submission"

    println(today)
    try {
      val userTableSchema = new StructType()
        .add("UserID", StringType, true)
        .add("signupDate", StringType, true)
        .add("userSegment", StringType, true)

      val visitorLogSchema = new StructType()
        .add("webClientID", StringType, true)
        .add("visitDateTime", StringType, true)
        .add("productID", StringType, true)
        .add("userID", StringType, true)
        .add("activity", StringType, true)
        .add("browser", StringType, true)
        .add("os", StringType, true)
        .add("city", StringType, true)
        .add("country", StringType, true)
      val sc = SparkSession.builder().appName(appName).master("local[3]").getOrCreate()


      println(sc.sparkContext.appName)
      val spark = sc.sparkContext
      // Load UserTable and VisitorLogTable
      val userTableDF = sc.read.option("header", true).
        option("inferSchema", true).
        schema(userTableSchema).
        csv(userTablePath).cache()
      val userTableFormatedDF = userTableDF.withColumn("signupDateUTC", to_timestamp(col("signupDate"))).drop("signupDate") //"yyyy-MM-dd HH:mm:ssz"

      //    print(userTableFormatedDF.show(false))

      val visitorLogDF = sc.read.option("header", true).
        option("inferSchema", true).
        schema(visitorLogSchema).
        csv(visitorLogDataPath)
      val visitorLogFormatedDF = visitorLogDF.withColumn("visitDateTimeUTC", to_timestamp(col("visitDateTime"))).drop("visitDateTime")
      //    print(visitorLogFormatedDF.show(false))

      // last 21 days data
      //      println(visitorLogFormatedDF.select(date_sub(max("visitDateTimeUTC"), 21).alias("date_last_21_days")).show())

      // register temp table
      visitorLogFormatedDF.createTempView("visitorTbl")
      userTableFormatedDF.createTempView("userTbl")

      // join query
      val joinQuery =
        s"""
           |SELECT u.UserID, u.signupDateUTC, u.userSegment,
           |       v.webClientID, v.visitDateTimeUTC,  lower(v.productID) AS productID, lower(v.browser) AS browser, lower(v.activity) AS activity, lower(v.os) AS os, lower(v.city) AS city, lower(v.country) AS country,
           |       to_date(v.visitDateTimeUTC) visitDate, to_date(u.signupDateUTC) signupDate
           | FROM  userTbl AS u LEFT JOIN  visitorTbl AS v
           | ON u.userID == v.userID
    """.stripMargin

      // join user tables
      //    val joinedDF = userTableFormatedDF.join( visitorLogFormatedDF, "userID").cache()
      val joinedDF = sc.sql(joinQuery).cache()
      val dropDuplicates = joinedDF.dropDuplicates("UserID")
      dropDuplicates.createTempView("finalTbl")
      //    println(joinedDF.show())
      //    joinedDF.repartition(1).write.option("header",true).csv("src/main/resources/preprocessed")
      // FEATURE GENERATION
      // No_of_days_Visited_7_Days
      val No_of_days_Visited_7_DaysQuery =
      """
        | SELECT t.UserID, COUNT( DISTINCT( t.visitDate )) as No_of_days_Visited_7_Days
        | FROM ( SELECT UserID , visitDate
        |         FROM  finalTbl
        |         WHERE (
        |               visitDate >=  '2018-05-21')
        |         ) t
        | GROUP BY t.UserID
        |""".stripMargin

      val num_days = sc.sql(No_of_days_Visited_7_DaysQuery)
      //      println(num_days.show())
      //No_Of_Products_Viewed_15_Days
      val No_Of_Products_Viewed_15_DaysQuery =
      """
        | SELECT t.UserID,
        | CASE
        |      WHEN COUNT(  t.productID )  IS NULL
        |      THEN 'Product101'
        |      ELSE COUNT(  t.productID )
        | END
        | AS No_Of_Products_Viewed_15_Days
        | FROM ( SELECT UserID , productID, activity
        |         FROM  finalTbl
        |         WHERE (
        |               visitDate >=  '2018-05-14' AND
        |               activity LIKE 'pageload'
        |               )
        |         ) t
        | GROUP BY t.UserID, t.productID
        |""".stripMargin
      val num_products = sc.sql(No_Of_Products_Viewed_15_DaysQuery)
      //    println(num_products.show())

      // User Vintage
      val User_VintageQuery =
        """
          |SELECT t.UserID, datediff( current_date(), t.signupDate  ) AS User_Vintage
          |FROM finalTbl t
          |GROUP BY t.UserID, t.signupDate
          |""".stripMargin


      val User_Vintage = sc.sql(User_VintageQuery)
      //    println(User_Vintage.show())

      // Most_Viewed_product_15_Days
      val Most_Viewed_product_15_DaysQuery =
        """
          |Select t.UserID, COUNT( t.productID ) AS Most_Viewed_product_15_Days
          |
          |FROM  ( SELECT UserID , visitDate, activity, productID
          |         FROM  finalTbl
          |         WHERE (
          |               visitDate >=  '2018-05-14')
          |         ) t
          |GROUP BY t.UserID
          |""".stripMargin
      val Most_Viewed_product_15_Days = sc.sql(Most_Viewed_product_15_DaysQuery)
      // Most_Active_OS
      val Most_Active_OSQuery =
        """
          |SELECT t.UserID, FIRST( t.os) AS Most_Active_OS
          |FROM finalTbl t
          |GROUP BY t.UserID
          |ORDER BY Most_Active_OS DESC
          |""".stripMargin
      val Most_Active_OS = sc.sql(Most_Active_OSQuery)
      //    println(Most_Active_OS.show)

      // Recently_Viewed_Product
      val Recently_Viewed_ProductQuery =
        """
          |SELECT t.UserID,
          |CASE
          |    WHEN t.productID IS NULL
          |    THEN 'Product101'
          |    ELSE t.productID
          |END Recently_Viewed_Product
          |FROM finalTbl t
          |WHERE t.activity LIKE 'pageload'
          |GROUP BY t.UserID, t.productID
          |""".stripMargin
      val Recently_Viewed_Product = sc.sql(Recently_Viewed_ProductQuery)
      //        println(Recently_Viewed_Product.show)

      // Clicks_last_7_days
      val Clicks_last_7_daysQuery =
        """
          |Select t.UserID, COUNT( t.activity ) AS Clicks_last_7_days
          |FROM  ( SELECT UserID , visitDate, activity
          |         FROM  finalTbl
          |         WHERE (
          |               visitDate >=  '2018-05-21')
          |         ) t
          |WHERE t.activity LIKE 'click'
          |GROUP BY t.UserID
          |""".stripMargin

      val Clicks_last_7_days = sc.sql(Clicks_last_7_daysQuery)
      //    println(Clicks_last_7_days.show)
      //    Clicks_last_7_days.repartition(1).write.option("header", true).csv("src/main/resources/clicks")

      // Pageloads_last_7_days
      val Pageloads_last_7_daysQuery =
        """
          |Select t.UserID, COUNT( t.activity ) AS Pageloads_last_7_days
          |FROM  ( SELECT UserID , visitDate , activity
          |         FROM  finalTbl
          |         WHERE (
          |               visitDate >=  '2018-05-21')
          |         ) t
          |WHERE t.activity LIKE 'pageload'
          |GROUP BY t.UserID
          |""".stripMargin
      val Pageloads_last_7_days = sc.sql(Pageloads_last_7_daysQuery)
      //    println(Pageloads_last_7_days.show)
      //    Pageloads_last_7_days.repartition(1).write.option("header", true).csv("src/main/resources/pageloads")
      // join all df to produce final df
      //    val JoinAllDF = num_days.join( num_products.withColumnRenamed("UserID", "Id") , col("UserID") === col("Id"), "inner").drop("Id").
      //                                join(User_Vintage.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id"), "inner").drop("Id").
      //                                join(Most_Viewed_product_15_Days.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id"), "inner").drop("Id").
      //                                join(Most_Active_OS.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id"), "inner").drop("Id").
      //                                join(Recently_Viewed_Product.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id"), "inner").drop("Id")
      //                                join(Clicks_last_7_days.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id") ).drop("Id")
      //                                join(Pageloads_last_7_days.withColumnRenamed("UserID", "Id") ,col("UserID") === col("Id"), "inner").drop("Id")

      //    println(JoinAllDF.show())
      val submissionDF = sc.sqlContext.read.option("header", true).csv("src/main/resources/sample_submission_M7Vpb9f.csv").withColumnRenamed("UserID", "Id").cache()
      //    println(submissionDF.show)
      submissionDF.createTempView("subTbl")
      num_days.createTempView("No_of_days_Visited_7_Days")
      num_products.createTempView("No_Of_Products_Viewed_15_Days")
      User_Vintage.createTempView("User_Vintage")
      Most_Viewed_product_15_Days.createTempView("Most_Viewed_product_15_Days")
      Most_Active_OS.createTempView("Most_Active_OS")
      Recently_Viewed_Product.createTempView("Recently_Viewed_Product")
      Clicks_last_7_days.createTempView("Clicks_last_7_days")
      Pageloads_last_7_days.createTempView("Pageloads_last_7_days")
      val finalQuery =
        """
          |SELECT a.Id AS UserID,
          |       b.No_of_days_Visited_7_Days,
          |       c.No_Of_Products_Viewed_15_Days,
          |       d.User_Vintage,
          |       e.Most_Viewed_product_15_Days,
          |       f.Most_Active_OS,
          |       g.Recently_Viewed_Product,
          |       h.Pageloads_last_7_days,
          |       i.Clicks_last_7_days
          |FROM subTbl a
          |LEFT JOIN No_of_days_Visited_7_Days b ON a.Id = b.UserID
          |LEFT JOIN No_Of_Products_Viewed_15_Days c  ON a.Id = c.UserID
          |LEFT JOIN User_Vintage d  ON a.Id = d.UserID
          |LEFT JOIN Most_Viewed_product_15_Days e  ON a.Id = e.UserID
          |LEFT JOIN Most_Active_OS f  ON a.Id = f.UserID
          |LEFT JOIN Recently_Viewed_Product g  ON a.Id = g.UserID
          |LEFT JOIN Pageloads_last_7_days h  ON a.Id = h.UserID
          |LEFT JOIN Clicks_last_7_days i  ON a.Id = i.UserID
          |ORDER BY a.Id
          |""".stripMargin
      //val finalQuery ="""
      //                  |SELECT a.Id,
      //                  |       b.No_of_days_Visited_7_Days,
      //                  |       c.No_Of_Products_Viewed_15_Days
      //                  |FROM subTbl a
      //                  |LEFT outer JOIN No_of_days_Visited_7_Days b ON a.Id = b.UserID
      //                  |LEFT JOIN No_Of_Products_Viewed_15_Days c  ON a.Id = c.UserID
      //                  |ORDER BY a.Id
      //                  |""".stripMargin

      val subDF = sc.sql(finalQuery)

      subDF.repartition(1).write.mode("Overwrite").option("header", true).csv(outputPath)
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      println( "Job Finished...")
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Had an IOException trying to read that file")
    }




  }
}