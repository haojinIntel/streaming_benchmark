package com.intel.streaming_benchmark.common

trait Schema {

  def getFieldNames: Array[String]

  def getFieldTypes: Array[String]


}

