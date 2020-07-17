package com.bbd

import com.bbd.common.hdfsRootPath

/**
  * @Author: maketubu
  * @Date: 2020/6/2 10:45
  */
object localTest {
  def main(args: Array[String]): Unit = {
    println(add_file_path("edge_person_airline_detail","2020",root = "person_relation"))
  }

  def add_file_path(filename: String,cp: String,root: String = "incrdir"): String={
    s"$hdfsRootPath/user/shulian/$root/$filename/cp=$cp"
  }
}
