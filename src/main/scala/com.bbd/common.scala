package com.bbd

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable
import com.bbd.utils.LongEncoding

import scala.collection.mutable.ListBuffer

object common {

  val hdfsRootPath = "hdfs://ngpcluster"
  val zookeeperQuorum = "sxsthm-173:2181,sxsthb-174:2181,sxsths3699:2181"
  val all_file_exprs = new mutable.HashMap[String,Array[String]]()
  private val callmsg_rowkey_expr: String = build_rowkey_expr("start_phone","end_phone","start_time","end_time")
  private val hotel_rowkey_expr: String = build_rowkey_expr("sfzh","lgdm","start_time","end_time")
  private val inter_rowkey_expr: String = build_rowkey_expr("sfzh","siteid","start_time","end_time")

  all_file_exprs += ("edge_groupcall_detail" -> Array(callmsg_rowkey_expr,"*"))
  all_file_exprs += ("edge_groupmsg_detail" -> Array(callmsg_rowkey_expr,"*"))
  all_file_exprs += ("edge_person_stay_hotel_detail" -> Array(hotel_rowkey_expr,"*"))
  all_file_exprs += ("edge_person_surfing_internetbar_detail" -> Array(inter_rowkey_expr,"*"))

  private val with_travel_expr: String = build_rowkey_expr("sfzh1","sfzh2","cp")
  private val with_interbar_expr: String = build_rowkey_expr("sfzh1","sfzh2","cp")
  private val person_inter_expr: String = build_rowkey_expr("sfzh1","sfzh2","start_time","end_time")
  private val person_train_expr: String = build_rowkey_expr("sfzh1","sfzh2","fcrq")
  private val person_airline_expr: String = build_rowkey_expr("sfzh1","sfzh2","hbrq")
  private val person_same_hotel_expr: String = build_rowkey_expr("sfzh1","sfzh2","start_time","end_time")
  private val person_photo_expr: String = build_rowkey_expr("zjhm")

  // 同出行上网明细 按月统计明细

  all_file_exprs += ("edge_person_train_detail" -> Array(person_train_expr,"*"))
  all_file_exprs += ("edge_person_airline_detail" -> Array(person_airline_expr,"*"))
  all_file_exprs += ("edge_person_internetbar_detail" -> Array(person_inter_expr,"*"))
  all_file_exprs += ("relation_withtrain_month" -> Array(with_travel_expr,"*"))
  all_file_exprs += ("relation_withair_month" -> Array(with_travel_expr,"*"))
  all_file_exprs += ("relation_withinter_month" -> Array(with_interbar_expr,"*"))
  all_file_exprs += ("vertex_person_photo" -> Array(person_photo_expr,"*"))
  all_file_exprs += ("edge_same_hotel_house_detail" -> Array(person_same_hotel_expr,"*"))

  private val person_label_expr = build_label_rowkey_expr("nodenum","person","date")
  private val phone_label_expr = build_label_rowkey_expr("nodenum","phone","date")
  private val imei_label_expr = build_label_rowkey_expr("nodenum","imei","date")

  all_file_exprs += ("person_labels" -> Array(person_label_expr,"*"))
  all_file_exprs += ("phone_labels" -> Array(phone_label_expr,"*"))
  all_file_exprs += ("imei_labels" -> Array(imei_label_expr,"*"))


  def build_rowkey_expr(id1:String,id2:String, start_time:String,end_time:String): String ={
    s"format_rowkey($id1,$id2,valid_timestamp(cast($start_time as string),cast($end_time as string))) rowkey"
  }

  def build_rowkey_expr(id1:String,id2:String, dotime:String): String ={
      s"format_rowkey($id1,$id2,cast($dotime as string)) rowkey"
  }

  def build_rowkey_expr(id1:String): String ={
    s"format_rowkey_sfzh($id1) rowkey"
  }

  def build_label_rowkey_expr(nodenum:String,label:String, date:String): String ={
    val longUnicode = LongEncoding.decode(label)
    s"format_label_rowkey($nodenum,'$longUnicode',cast($date as string)) rowkey"
  }



  def add_file_path(filename: String,cp: String,root: String = "incrdir"): String={
    s"$hdfsRootPath/user/shulian/$root/$filename/cp=$cp"
  }

  def add_save_path(filename: String,cp:String): String={
    s"$hdfsRootPath/user/shulian/bulkload_hfiles/$filename/cp=$cp"
  }

  def getBetweenDays(start_cp:String,end_cp:String): List[String] = {
    val dateformat = new SimpleDateFormat("yyyyMMdd")
    val start_date = dateformat.parse(start_cp)
    val end_date = dateformat.parse(end_cp)
    var cps = new ListBuffer[String]
    cps += dateformat.format(start_date.getTime)
    val tmpstart = Calendar.getInstance()
    tmpstart.setTime(start_date)
    tmpstart.add(Calendar.DAY_OF_YEAR,1)
    val tmpend = Calendar.getInstance()
    tmpend.setTime(end_date)
    while (tmpstart.before(tmpend)){
      cps += dateformat.format(tmpstart.getTime)
      tmpstart.add(Calendar.DAY_OF_YEAR,1)
    }
    cps.map(t => t+"00").toList
  }


  def get_habse_conf(tablename:String): Configuration ={
    val conf = HBaseConfiguration.create() //Hbase配置信息
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum) //Hbase zk信息
    conf.set("hbase.mapreduce.hfileoutputformat.table.name", s"SLMP:$tablename") //Hbase 输出表
    conf.set("hbase.unsafe.stream.capability.enforce", "false") //hbase  根目录设定  （有时候会报错，具体看错误处理部分）
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.fs.tmp.dir","/user/shulianmingpin/hbase-stage")
    conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200")
    conf.set("hbase.hregion.max.filesize","10737418240")
    conf
  }

  def get_habse_conf(): Configuration ={
    val conf = HBaseConfiguration.create() //Hbase配置信息
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum) //Hbase zk信息
    conf.set("hbase.unsafe.stream.capability.enforce", "false") //hbase  根目录设定  （有时候会报错，具体看错误处理部分）
    conf.set("zookeeper.znode.parent", "/hbase")
    conf
  }



}
