package com.ETL.utils

trait Tag {

  /**
    * 打标签的出口
    */
  def makeTags(args:Any*):List[(String,Int)]

}
