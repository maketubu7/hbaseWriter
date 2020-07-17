package com.bbd

/**
  * @Author: maketubu
  * @Date: 2020/6/1 14:35
  * @discribe: 很久更新一次的数据
  */

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.bbd.udf_case.register_all_udf
import com.bbd.common.get_habse_conf
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import com.bbd.services._

class loadAllData{}
object loadAllData {

  val logger = Logger.getLogger(classOf[loadAllData])
  val root_dir = "person_relation"
  val sparkconf = new SparkConf()
  sparkconf.set("spark.executor.instances","100")
  sparkconf.set("spark.executor.memory", "25g")
  sparkconf.set("spark.executor.cores", "4")
  sparkconf.set("spark.executor.memoryOverhead", "5g")
  sparkconf.set("spark.default.parallelism", "2500")
  sparkconf.set("spark.sql.shuffle.partitions", "2500")
  sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkconf.set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ExportToHBase")
    .config(conf=sparkconf)
    .enableHiveSupport()
    .getOrCreate()

  register_all_udf(spark)
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val cps = args(1).toString.split(",")
    val filenames = args(0).toString.split(",")
    for (file <- filenames){
      for (cp <- cps) {
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

}
