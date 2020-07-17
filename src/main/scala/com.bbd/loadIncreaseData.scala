package com.bbd

/**
  * @Author: maketubu
  * @Date: 2020/6/1 14:35
  * @discribe: 每天的明细增量数据
  */
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.bbd.udf_case.register_all_udf
import com.bbd.services._
import com.bbd.common.get_habse_conf
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

class loadIncreaseData{}
object loadIncreaseData {

  val logger = Logger.getLogger(classOf[loadIncreaseData])
  val root_dir = "incrdir"
  val sparkconf = new SparkConf()
  sparkconf.set("spark.executor.instances","100")
  sparkconf.set("spark.executor.memory", "20g")
  sparkconf.set("spark.executor.cores", "4")
  sparkconf.set("spark.executor.memoryOverhead", "10g")
  sparkconf.set("spark.default.parallelism", "2000")
  sparkconf.set("spark.sql.shuffle.partitions", "2000")
  sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkconf.set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ExportToHBase")
    .config(conf=sparkconf)
    .enableHiveSupport()
    .getOrCreate()

  // 生成rowkey的udf
  register_all_udf(spark)
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val cp = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd00"))
//    val cp = args(0).toString
    val filenames = List("edge_person_stay_hotel_detail","edge_groupcall_detail",
              "edge_groupmsg_detail","edge_person_surfing_internetbar_detail")
    for (file <- filenames){
      val conf = get_habse_conf(file.toUpperCase)
      var HFile: RDD[(ImmutableBytesWritable, KeyValue)] = null
      try {
        HFile = getHFileRDD(spark,file,cp,root_dir)
      }
      catch {
        case e:Exception => logger.error(e.printStackTrace())
        case e:Exception => logger.info(s"$file 在当前分区 $cp 没有数据")
      }
      if (HFile != null){
        saveHfile(HFile,file,cp,conf)
        loadHFileToHbase(conf,file,cp)
        logger.info(s"$file $cp dobulkload success")
      }
    }
  }
}
