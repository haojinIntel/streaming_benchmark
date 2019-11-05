package com.intel.streaming_benchmark.common

case class Column(
    name: String,
    index: Int,
    types: String

)

trait TcpDsSchema extends Schema {

  val columns: Array[Column]

  def getFieldNames: Array[String] = columns.map(_.name)

  def getFieldTypes: Array[String] =
    columns.map(column => column.types)

}

object Shopping extends TcpDsSchema {

  override val columns = Array[Column](
    Column("userId", 0, "String"),
    Column("commodity", 1, "String"),
    Column("times", 2, "LONG")
  )
}

object Click extends TcpDsSchema {

  override val columns = Array[Column](
    Column("click_time", 0, "Long"),
    Column("strategy", 1, "String"),
    Column("site", 2, "String"),
    Column("pos_id", 3, "String"),
    Column("poi_id", 4, "String"),
    Column("device_id", 5, "String")
  )
}

object Imp extends TcpDsSchema {

  override val columns = Array[Column](
    Column("imp_time", 0, "Long"),
    Column("strategy", 1, "String"),
    Column("site", 2, "String"),
    Column("pos_id", 3, "String"),
    Column("poi_id", 4, "String"),
    Column("cost", 5, "Double"),
    Column("device_id", 6, "String")
  )
}

object Dau extends TcpDsSchema {

  override val columns = Array[Column](
    Column("dau_time", 0, "Long"),
    Column("device_id", 1, "String")
  )
}

object UserVisit extends TcpDsSchema {

  override val columns = Array[Column](
    Column("date", 0, "String"),
    Column("userId", 1, "Long"),
    Column("sessionId", 2, "String"),
    Column("pageId", 3, "Long"),
    Column("actionTime", 4, "String"),
    Column("searchKeyword", 5, "String"),
    Column("clickCategoryId", 6, "String"),
    Column("clickProductId", 7, "String"),
    Column("orderCategoryIds", 8, "String"),
    Column("orderProductIds", 9, "String"),
    Column("payCategoryIds", 10, "String"),
    Column("payProductIds", 11, "String"),
    Column("cityId", 12, "String")
  )
}

object TableSchemaProvider {
  val schemaMap: Map[String, Schema] = Map(
    "shopping" -> Shopping,
    "click" -> Click,
    "imp" -> Imp,
    "dau" -> Dau,
    "userVisit" -> UserVisit
  )

  def getSchema(tableName: String): Schema = {
    if (schemaMap.contains(tableName)) {
      schemaMap(tableName)
    } else {
      throw new IllegalArgumentException(s"$tableName does not exist!")
    }
  }

}
