import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.expressions._
import org.apache.hadoop.fs._
import org.apache.log4j.{LogManager, Logger}
import org.apache.log4j._

import java.io.FileNotFoundException

//import java.util.logging.LogManager

//Developpement d'applications Big Data en Spark

object SparkBigData {

  var ss : SparkSession = null
  //var spConf : SparkConf = null

  private val trace_log : Logger = LogManager.getLogger( "Logger_Console")

  val schema_orders = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("campaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("payementtype", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("numorderlines", IntegerType, true),
    StructField("numunit", IntegerType, true)
  ))


  val schema_df_test = StructType(Array(
    StructField("_c0", StringType, true),
    StructField("InvoiceNo", IntegerType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", DateType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", StringType, true),
    StructField("Country", StringType, true),
    StructField("InvoiceTimestamp", TimestampType, true)
  ))


  def main(args: Array[String]) : Unit = {
    val session_s = Session_Spark(true)
    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header", "true")
      .schema(schema_df_test)
      .csv(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2010-12-06.csv")
    df_test.show(5)
    df_test.printSchema()

    val df_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\csvs\\*")

    //df_gp.show(7)
    //println("df_test count :" + df_test.count() + " df_group count : " + df_gp.count())

    val df_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(paths = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2010-12-06.csv", "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2011-12-08.csv")

    //df_gp2.show(7)
    //println("df_gp count :" + df_gp.count() + " df_group2 count : " + df_gp2.count())

   // df_test.printSchema()

    val df_2 = df_test.select(
      col("_c0").alias("ID du client"),
      col("InvoiceNo").cast(StringType),
      col("StockCode").cast(IntegerType).alias("code de la marchandise"),
      col("Invoice".concat("No")).alias("ID de la commande")
    )
    //df_2.show(4)

    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType) )
      .withColumn("valeur_constante", lit(50))
      .withColumnRenamed("_c0", "ID_client")
      .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("Created_dt", current_timestamp())
      .withColumn("reduction_test", when(col("total_amount") > 15, lit(3)).otherwise(lit(0)))
      .withColumn("net_income", col("total_amount") - col("reduction_test"))
      .withColumn("reduction", when(col("total_amount") < 15, lit(0))
                                        .otherwise(when(col("total_amount").between(15, 20), lit(3))
                                        .otherwise(when(col("total_amount") > 15, lit(4)))))

    val df_notreduced = df_3.filter((col("reduction") === lit(0)) && col("Country").isin("United Kingdom", "France", "USA"))

    //df_3.show(15)
    //df_notreduced.show(5)

    //jointures de data frame
    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header", "true")
      .schema(schema_orders)
      .load(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\orders.txt")

    //df_orders.show(10)
    //df_orders.printSchema()

    val df_ordersGood = df_orders.withColumnRenamed("numunits", "numunits_orders")
      .withColumnRenamed("totalprice", "totalprice_orders")

    val df_product = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header", "true")
      .load(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\product.txt")
    //df_product.show(5)
    //df_product.printSchema()

    val df_orderline = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header", "true")
      .load(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\orderline.txt")

    //df_orderline.show(5)
    //df_orderline.printSchema()

    val df_joinOrders = df_orderline.join(df_ordersGood, df_orderline.col("orderid") === df_ordersGood.col("orderid"), "inner")
      .join(df_product, df_product.col("productid") === df_orderline.col("productid"), Inner.sql)
      //.show(5)
    df_joinOrders.printSchema()

    val df_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header", "true")
      .csv(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2010-12-06.csv")

    val df_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header", "true")
      .csv(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2011-01-20.csv")

    val df_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header", "true")
      .csv(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\sources de données\\2011-12-08.csv")

    val df_unitedfiles = df_fichier1.union(df_fichier2.union(df_fichier3))
      //println(df_fichier3.count() + " " +  df_unitedfiles.count())

    df_joinOrders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("city")
      .sum("total_amount").as("Commandes totales")
      //.show()

    //opérations de fénêtrage
    val win_specs = Window.partitionBy(col("state"))
    val df_windows = df_joinOrders.withColumn("ventes_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(win_specs))
      .select(
        col("orderlineid"),
        col("zipcode"),
        col("PRODUCTGROUPNAME"),
        col("state"),
        col("ventes_dep").alias("Ventes_par_département")
      )//.show(10)
    //df_windows.show(5)

    //manipulation des dates et du temps en spark
    df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MM/YYYY"))
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_jour", window(col("orderdate"), "2 days" ))
      .select(
        col("orderdate"),
        col("periode_jour"),
        col("periode_jour.start"),
        col("periode_jour.end")
      )
      //.show(10)
    df_unitedfiles.show(10)
    df_unitedfiles.printSchema()

    df_unitedfiles.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
      //.withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceTimestamp")))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
      .withColumn("Invoice_add_2month", add_months(col("InvoiceDate"), 2))
      .withColumn("Invoice_add_date", date_add(col("InvoiceDate"), 30))
      .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 25))
      .withColumn("Invoice_date_diff", datediff(current_date(), col("InvoiceDate")))
      .withColumn("InvoiceDateQuater", quarter(col("InvoiceDate")))
      .withColumn("InvoiceDate_id", unix_timestamp(col("InvoiceDate")))
      .withColumn("InvoiceDate_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd-MM-yyyy"))
      //.show(10)
      //.printSchema()

    //df_ordersGood.show(10)

    df_product
      .withColumn("productGp", substring(col("PRODUCTGROUPNAME"), 2, 2) )
      .withColumn("productln", length(col("PRODUCTGROUPNAME")))
      .withColumn("concat_product", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
      //.where(regexp_extract(trim(col("PRODUCTID")), "[0-9]{5}",0) ==trim(col("PRODUCTID")))
      .where(! col("PRODUCTID").rlike("[0-9]{5}"))
      //.count()
      //.show(10)

    //Utilisation UDF
    //import session_s.implicits._
    //val  phone_list : DataFrame = List("0709789485", "+3307897025", "8794087834").toDF("phone_list")

   // phone_list
    //  .withColumn("test_phone", valid_phoneUDF(col("phone_number")))
     // .show()


    /*df_windows
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\Ecriture")
      //.save("C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\Ecriture")
*/
    //exemplede propriétés d'un format
    //df_2.write.format("orc")
     // .option("orc.bloom.filter.columns", "favorite_color")
      //.option("orc.dictionary.key.threshold", "1.0")
     // .option("orc.column.encoding.direct", "name")
     // .orc("users_with_options.orc")


    //Utilisation du sql dans spark
    df_joinOrders.createOrReplaceTempView("orders")

    val df_sql : DataFrame = session_s.sql("""
    select state, city, sum(round(numunits * totalprice)) as commandes_totales from orders group by state, city
    """)
      df_sql.show(20)

    val df_hive = session_s.table("orders") //lire une table à partir du metastore Hive
      df_sql.write.mode(SaveMode.Overwrite).saveAsTable("report_orders") //enregistrer et écrire un  DataFrame dans le metastore Hive


  }

  //def valid_phone(phone_to_test : String) : Unit = {

   // var result: Boolean = false
   // val motif_regexp = "^0[0-9]{9}".r
    //if (motif_regexp.findAllIn(phone_to_test.trim) == phone_to_test.trim) {
    //  result = true
    //} else {
    //  result = false
    //}
   // return  result
  //}

 // val valid_phoneUDF : UserDefinedFunction = udf{phone_to_test : String => valid_phone(phone_to_test : String)}



  def spark_hdfs () : Unit = {

    val config_fs = Session_Spark(true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_path = new Path("/user/datalake/marketing/")
    val dest_path = new Path("/user/datalake/indexes")
    val ren_src = new Path(("/user/datalake/marketing/fichier_reporting.parquet"))
    val dest_src = new Path(("/user/datalake/marketing/reporting.parquet"))
    val local_path = new Path("C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame\\Ecriture\\parts.csv")
    val path_local = new Path("C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Data frame")

    //lecture des fichiers d'un dossier
    val files_list = fs.listStatus(src_path)
    files_list.foreach(f =>println(f.getPath))

    val files_list1 = fs.listStatus(src_path).map(x => x.getPath)
    for ( i <- 1 to files_list1.length) {
      println( files_list1(i))
    }

    //renommage des fichiers
    fs.rename(ren_src, dest_src)

    //supprimer des fichier dans un dossier
    fs.delete(dest_src,true)

    //copier des fichiers
    fs.copyFromLocalFile(local_path, dest_path)
    fs.copyToLocalFile(dest_path, path_local)


  }


  def mainip_rdd(): Unit = {

    val session_s = Session_Spark(true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")

    val rdd_test : RDD[String] = sc.parallelize(List("alain", "Juvenal", "abel", "julien", "anna"))
    rdd_test.foreach{
      l => println(l)
    }
    println()

    val rdd2 : RDD[String] = sc.parallelize(Array("Lucie", "fabien", "jules"))
    rdd2.foreach{
      l => println(l)
    }
    println()

    val rdd3 = sc.parallelize(Seq(("julien","Math", 15), ("Aline", "Math", 17), ("Juvénal", "Math", 19), ("Abel", "Math", 18)))
    println("Premier element de mon RDD 3")
    rdd3.take(num = 1).foreach{
      l => println(l)
    }
    println()

    if(rdd3.isEmpty()) {
      println("Le RDD est vide")
    } else {
        rdd3.foreach{
          l => println(l)
        }
      }
    println()

    rdd3.saveAsTextFile(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\rdd.txt")
    rdd3.repartition(numPartitions = 1).saveAsTextFile(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\rdd3.txt")

    //rdd3.collect().foreach{ l => println(l)}

    // Création  d'un RDD à partir d'une source de données
    val rdd4 = sc.textFile(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Sources\\textRDD.txt")
    println("lecture du contenu du RDD 4")
   // rdd4.foreach{ l => println(l)}
    println()

    val rdd5 = sc.textFile(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Sources\\*")
    println("lecture du contenu du RDD 5")
    //rdd5.foreach{ l => println(l)}
    println()

    //transformation des RDD
    val rdd_trans : RDD[String] = sc.parallelize(List("alain mange une banane", "La banane est un bon aliment pour la santé", "acheter une bonne banane"))
    //rdd_trans.foreach(l => println("ligne de mon RDD : " +l))

    val rdd_map = rdd_trans.map(x => x.split( " "))
    println("Nbr d'éléments de mon RDD Map : " + rdd_map.count())

    val rdd6 = rdd_trans.map(w => (w, w.length, w.contains("banane"))).map(x => (x._1.toLowerCase(), x._2, x._3))
    println("RDD 6 est: ")
    rdd6.foreach(l => println(l))

    println("RDD 7 est: ")
    val rdd7 = rdd6.map(x => (x._1.toUpperCase(), x._2, x._3))
    rdd7.foreach(l => println(l))

    println("RDD 8 est: ")
    val rdd8 = rdd6.map(x => (x._1.split(" "), 1))
    //rdd8.foreach(l => println(l._1(0), l._2))

    println("RDD  fm est: ")
    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(w => (w,1))
    //rdd_fm.foreach(l => println(l))

    val rdd_compte = rdd5.flatMap(x => x.split(" ")).map(m => (m, 1)).reduceByKey((x, y) => x + y)
    //rdd_compte.repartition(1).saveAsTextFile(path = "C:\\Users\\abel\\Desktop\\Formation_BigDataStreaming_Juv\\Maitriser_spark_BigData_leadSpark\\ressources\\Sources\\comptageRDD.txt")
    //rdd_compte.foreach(l => println(l))

    val rdd_filtered = rdd_fm.filter(x => x._1.equals("banane"))
    rdd_filtered.foreach(l => println(l))

    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y)
    rdd_reduced.foreach(l => println(l))

    rdd_fm.cache()
    //rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_fm.unpersist()

    import session_s.implicits._
    val df : DataFrame = rdd_fm.toDF("texte", "valeur")
    df.show(50)






  }

  /**
   * fonction qui initialise et instancie une session spark
   * @param Env : c'est une variable qui indique l'environnement sur lequel notre application est déployée
   *            si Env = true, alors l'application est déployée en local, sinon, elle est déployée sur un cluster
   */

  def Session_Spark(Env : Boolean = true) : SparkSession = {
    trace_log.info("initialisation du contexte Spark Streaming")
    try {
      if (Env == true) {
        System.setProperty("hadoop.home.dir", "C:/Hadoop") //à logger
        ss = SparkSession.builder()
          .master(master = "local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      } else {
        ss = SparkSession.builder()
          .appName(name = "Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }
    } catch {
      case ex : FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué" + ex.printStackTrace())
      case ex : Exception => trace_log.error("Erreur dans l'initialisation de la session Spark" + ex.printStackTrace())
    }
    ss

  }




}
