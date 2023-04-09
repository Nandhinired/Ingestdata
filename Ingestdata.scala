package Ingestdata
import org.apache.spark.sql.SparkSession
object Ingestdata extends App {
  override def main(agrs:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(name = "GCP Data Ingestion")
      .getOrCreate()

    val gcsPath = "gs://first_dag_bucket/raw/yellow_tripdata/2022/yellow_tripdata_2022-01.parquet"

    // val gcsPath = "C:/Users/nandh/Desktop/BigData/parquet_output/part-00000-16718f6d-5547-426b-8bf9-b6c795cc215d-c000.snappy.parquet"
    val input = spark.read.parquet(gcsPath)

    input.show(4)

    val outPath = "gs://first_dag_bucket/raw/yellow_tripdata/2022/"
    input.write
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .mode("append")
      .save(outPath)



  }

}
