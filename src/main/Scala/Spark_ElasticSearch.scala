import SparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util._
import org.elasticsearch.spark.sql._
import org.apache.commons.httpclient.HttpConnectionManager
import org.apache.commons.httpclient._


object Spark_ElasticSearch {

  def main(args: Array[String]): Unit = {

    val ss = SparkBigData.Session_Spark(Env = true)

    val df_orders = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\orders_csv.csv")

    //df_orders.show(15)

df_orders.write //ecriture des données dans un index ElasticSearch
  .format("org.elasticsearch.spark.sql")
  .mode(SaveMode.Append)
  .option("es.port", "9200")
  .option("es.nodes", "localhost")
  .option("es.net.http.auth.user", "elastic")
  .option("es.net.http.auth.pass", "GBbjod5O7INV1O8WwVPc")
  .save("index_abt/doc")


/*
    //autre façon de faire
   val session_s = SparkSession.builder()
      .appName(name = "Mon application Spark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("es.port", "9200")
      .config("es.nodes", "localhost")
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "GBbjod5O7INV1O8WwVPc")
      .enableHiveSupport()

    df_orders.saveToEs("index_abt/doc")
    */



  }

}
