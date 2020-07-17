package com.bbd

import com.bbd.common.{add_file_path, add_save_path, all_file_exprs, get_habse_conf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object services {
  private val cf = "f"
  def getHFileRDD(spark:SparkSession,filename: String,cp:String,root:String): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val key = "rowkey"
    //列族
    val clounmFamily: String = cf
    var sourcePath = ""
    if (filename.contains("hotel")) {
      sourcePath = add_file_path(filename,cp)
    }
    else{
      sourcePath = add_file_path(filename,cp,root)
    }

    var SourceDataFrame = spark.read.orc(sourcePath).selectExpr(all_file_exprs(filename): _*).dropDuplicates("rowkey")
    if (SourceDataFrame.dtypes.map(x => x._1).contains("tablename")){
      SourceDataFrame = SourceDataFrame.drop("tablename")
    }

    val columnsName: Array[String] = SourceDataFrame.columns.drop(1).sorted
    val res_tmp: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = SourceDataFrame
      .rdd
      .map(f = row => {
        var kvlist: Seq[KeyValue] = List() //存储多个列
        var kv: KeyValue = null
        val cf: Array[Byte] = Bytes.toBytes(clounmFamily) //列族
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](key))
        val immutableRowKey: ImmutableBytesWritable = new ImmutableBytesWritable(rowKey)
        for (i <- columnsName.indices) {
          var value: Array[Byte] = null
          try {
            value = Bytes.toBytes(row.getAs[String](columnsName(i)))
          } catch {
            case e: ClassCastException =>
              value = Bytes.toBytes(row.getAs[Object](columnsName(i)).toString)
            case e: Exception =>
              e.printStackTrace()
          }
          kv = new KeyValue(rowKey, cf, Bytes.toBytes(columnsName(i)), value)
          kvlist = kvlist :+ kv
        }
        (immutableRowKey, kvlist)
      })

    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = res_tmp.flatMapValues(_.iterator)
    hfileRDD
  }

  def getHFileRDD(spark:SparkSession,filenames: Array[String],cp:String,root:String): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val key = "rowkey"
    //列族
    val clounmFamily: String = cf
    var sourcePath = ""
    var allData : DataFrame= null
    for (file <- filenames){
      sourcePath = add_file_path(file,cp,root)
      val SourceDataFrame: Dataset[Row] = spark.read.orc(sourcePath).selectExpr(all_file_exprs(file): _*).dropDuplicates("rowkey")
      if (allData != null){
        allData = allData.union(SourceDataFrame)
      }
      else{
        allData = SourceDataFrame
      }
    }

    val columnsName: Array[String] = allData.columns.drop(1).sorted
    val res_tmp: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = allData
      .rdd
      .map(f = row => {
        var kvlist: Seq[KeyValue] = List() //存储多个列
        var kv: KeyValue = null
        val cf: Array[Byte] = Bytes.toBytes(clounmFamily) //列族
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](key))
        val immutableRowKey: ImmutableBytesWritable = new ImmutableBytesWritable(rowKey)
        for (i <- columnsName.indices) {
          var value: Array[Byte] = null
          try {
            value = Bytes.toBytes(row.getAs[String](columnsName(i)))
          } catch {
            case e: ClassCastException =>
              value = Bytes.toBytes(row.getAs[Object](columnsName(i)).toString)
            case e: Exception =>
              e.printStackTrace()
          }
          kv = new KeyValue(rowKey, cf, Bytes.toBytes(columnsName(i)), value)
          kvlist = kvlist :+ kv
        }
        (immutableRowKey, kvlist)
      })

    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = res_tmp.flatMapValues(_.iterator)
    hfileRDD
  }

  def saveHfile(hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)],filename:String,cp:String,conf:Configuration): Unit = {

    val savePath = add_save_path(filename,cp)
    delete_hdfspath(savePath)
    hfileRDD
      .sortBy(tuple => (tuple._1,tuple._2.getKeyString),true) //要保持 整体有序
      .saveAsNewAPIHadoopFile(savePath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        conf)
  }

  def loadHFileToHbase(conf:Configuration, filename:String,cp:String): Unit = {

    val namespace = "SLMP:"
    createTable(namespace+filename.toUpperCase,cf)
    val load: LoadIncrementalHFiles = new LoadIncrementalHFiles(conf)
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val table: Table = conn.getTable(TableName.valueOf(namespace+filename.toUpperCase))

    val regionLocator: RegionLocator = conn.getRegionLocator(TableName.valueOf(namespace+filename.toUpperCase))
    val job: Job = Job.getInstance(conf)
    job.setJobName(s"$filename LoadIncrementalHFiles")

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
    val savePath = add_save_path(filename,cp)
    load.doBulkLoad(new Path(savePath), conn.getAdmin, table, regionLocator)

  }

  def delete_hdfspath(url: String) {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path: Path = new Path(url)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  def createTable(tablename:String,colfamily:String): Unit ={
    val conf = get_habse_conf()
    val conn = ConnectionFactory.createConnection(conf)

    val tn = TableName.valueOf(tablename)
    var admin:Admin = null
    try{
      admin = conn.getAdmin
      val flag = admin.isTableAvailable(tn)
      if (!flag){
        val tableDesc = new HTableDescriptor(tablename)
        tableDesc.addFamily(new HColumnDescriptor(colfamily.getBytes()))
        admin.createTable(tableDesc)
      }
    }
    catch {
      case e:Exception => Unit
    }
    finally {
      conn.close()
    }
  }
}
