import SparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.spark._


object Spark_HBase {

  def catalog_orders = s"""{
                      "table":{"namespace":"default", "name":"table_orders"},
                      "rowkey":"key",
                      "columns":{
                      "order_id":{"cf":"rowkey", "col":"key", "type":"string"},
                      "customer_id":{"cf":"orders", "col":"customerid", "type":"string"},
                      "campaign_id":{"cf":"orders", "col":"campaignid", "type":"string"},
                      "order_date":{"cf":"orders", "col":"order_date", "type":"string"},
                      "city":{"cf":"orders", "col":"city", "type":"string"},
                      "state":{"cf":"orders", "col":"state", "type":"string"},
                      "zipcode":{"cf":"orders", "col":"zipcode", "type":"string"},
                      "paymenttype":{"cf":"orders", "col":"paymenttype", "type":"string"},
                      "totalprice":{"cf":"orders", "col":"totalprice", "type":"string"},
                      "numorderlines":{"cf":"orders", "col":"numorderlines", "type":"string"},
                      "numunits":{"cf":"orders", "col":"numunits", "type":"string"}
                      }
                      }""".stripMargin

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(Env = true)

    val df_hbase = ss.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog_orders))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    df_hbase.printSchema()
    df_hbase.show(false)

    df_hbase.createOrReplaceTempView("Orders")

    ss.sql("select * from Orders where state = 'MA'").show()

  }

}
