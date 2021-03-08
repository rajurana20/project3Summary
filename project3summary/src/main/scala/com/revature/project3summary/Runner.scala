package com.revature.project3summary

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType
import com.google.flatbuffers.Struct
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import scala.io.BufferedSource
import java.io.FileInputStream
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round 
import java.util.Arrays
import java.sql
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("local[4]")
      // .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //Read WET data from the Common Crawl s3 bucket
    //Since WET is read in as one long text file, use lineSep to split on each header of WET record
    val fileName = "s3a://rajusecondbucket/crawlresult/"

    case class CrawlResult (
        fileName:StringType,
        totalTechJob:Long,
        totalEntryLevelTechJob:Long,
        totalEntryLevelTechJobRequiredExperience: Long,
        percentage: Double
    ){}
    println("--------------Summary Table---------------")
    val commonCrawl = spark.read.option("delimiter",",").csv(fileName)
    //.as[CrawlResult]
    .createOrReplaceTempView("crawlresult")
    val query=spark.sql("select _c0 as file_name, _c1 as total_tech_job, _c2 as total_entry_tech_job, _c3 as total_entry_tech_job_required_experience,_c4 as percentage from crawlresult");
    query.show()
    
    println("Average Percentage of tech job that required experiece to total tech job is : ")
     val query2=spark.sql("select avg(_c4) as average_percentage from crawlresult")
     query2.show();
  }

}