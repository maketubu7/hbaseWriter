package com.bbd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.util.control._

/**
  * @Author: maketubu
  * @Date: 2020/6/1 15:46
  */

object udf_case {

  def reverse_string(data:String): String = {
    new StringBuilder(data).reverse.toString
  }

  def format_rowkey(start_phone:String,end_phone: String,start_time:String): String ={
    val r1 = reverse_string(start_phone)
    val r2 = reverse_string(end_phone)
    "%s_%s_%s".format(r1,r2,start_time)
  }

  def format_label_rowkey(node_num:String,label: String,start_time:String): String ={
    val r1 = reverse_string(node_num)
    "%s_%s_%s".format(r1,label,start_time)
  }

  def format_rowkey_sfzh(sfzh:String): String ={
    reverse_string(sfzh)
  }

  def valid_timestamp(start_time: String, end_time:String): String = {
    var res = ""
    val times = Array(start_time,end_time)
    val loop = new Breaks;
    loop.breakable {
      for (time <- times) {
        if (time != "0" && time != "") {
          res = time
          loop.break
        }
      }
    }
    if (res == ""){
      res = (new Date().getTime.toString.substring(0,10).toLong + 622080000).toString
    }
    res
  }

  def register_all_udf(spark:SparkSession): Unit ={
    spark.udf.register("format_rowkey",format_rowkey _)
    spark.udf.register("valid_timestamp",valid_timestamp _)
    spark.udf.register("format_rowkey_sfzh",format_rowkey_sfzh _)
    spark.udf.register("format_label_rowkey",format_label_rowkey _)
  }

}
