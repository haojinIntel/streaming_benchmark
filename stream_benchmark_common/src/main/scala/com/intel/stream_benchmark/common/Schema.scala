package com.intel.stream_benchmark.common

trait Schema {

  def getFieldNames: Array[String]

  def getFieldTypes: Array[String]


}

