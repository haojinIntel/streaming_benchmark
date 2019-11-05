package com.intel.streaming_benchmark

import java.util.Random


object click {

  val random = new Random
  val splitSymbol = ","

  val userNumbers = 1000

  val userVisitSessionNumbers = 10000

  val productNumbers = 10000

  val productCategoryNumbers = 50

  val professionals = Array("Programmer", "Teacher", "Cook", "Driver", "Doctor", "Nurse", "Designer", "Farmer", "Worker", "Assistant")
  val professionalTypeSize = professionals.length

  val citys: Array[(Int, String)] = Array("Shanghai", "Beijing", "Shenzhen", "Guangzhou", "Nanjing", "Hangzhou", "Changsha", "Nanchang", "Zhangjiajie", "Hong Kong", "Macao").zipWithIndex.map(_.swap)
  val cityTypeSize = citys.length

  val sexs = Array("male", "female", "unknown")
  val sexTypeSize = sexs.length
  // search key word
  val keywords = Array("Hot Pot", "Cake", "Chongqing spicy chicken", "Chongqing facet",
    "Biscuits", "Fish", "International Trade Building or Cetra Building", "Pacific Mall", "Japanese cuisine", "Hot Spring")
  val keywordSize = keywords.length

  var count = 0
}


case class ProductInfo(
                        productID: Long,
                        productName: String,
                        extendInfo: String
                      ) {
  /**
    * Format
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = "^"): String = {
    s"${productID}${splitSymbol}${productName}${splitSymbol}${extendInfo}"
  }
}

object ProductInfo {
  /**
    * column name of the table
    */
  val columnNames = Array("product_id", "product_name", "extend_info")

  /**
    * Parse row data and return the object; if parsing fails return None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseProductInfo(line: String, splitSymbol: String = "\\^"): Option[ProductInfo] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 3) {
      Some(
        new ProductInfo(
          arr(0).toLong,
          arr(1),
          arr(2)
        )
      )
    } else None
  }
}



case class UserInfo(
                     userId: Long,
                     userName: String,
                     name: String,
                     age: Int,
                     professional: String,
                     city: String,
                     sex: String
                   ) {
  /**
    * Format time
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = ","): String = {
    s"${userId}${splitSymbol}${userName}${splitSymbol}${name}${splitSymbol}${age}${splitSymbol}${professional}${splitSymbol}${city}${splitSymbol}${sex}"
  }
}

object UserInfo {
  /**
    * column name of the table
    */
  val columnNames = Array("user_id", "user_name", "name", "age", "professional", "city", "sex")

  /**
    * Parse row data and return the object; if parsing fails return None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseUserInfo(line: String, splitSymbol: String = ","): Option[UserInfo] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 7) {
      Some(new UserInfo(
        arr(0).toLong,
        arr(1),
        arr(2),
        arr(3).toInt,
        arr(4),
        arr(5),
        arr(6)
      ))
    } else None
  }
}


case class UserVisitAction(
                            date: String,
                            userId: Long,
                            sessionId: String,
                            pageId: Long,
                            actionTime: Long,
                            searchKeyword: String,
                            clickCategoryId: String,
                            clickProductId: String,
                            orderCategoryIds: String,
                            orderProductIds: String,
                            payCategoryIds: String,
                            payProductIds: String,
                            cityId: Int
                          ) {
  /**
    * Format time
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String = ","): String = {
    s"${date}${splitSymbol}${userId}${splitSymbol}${sessionId}${splitSymbol}${pageId}${splitSymbol}${actionTime}${splitSymbol}${searchKeyword}${splitSymbol}${clickCategoryId}${splitSymbol}${clickProductId}${splitSymbol}${orderCategoryIds}${splitSymbol}${orderProductIds}${splitSymbol}${payCategoryIds}${splitSymbol}${payProductIds}${splitSymbol}${cityId}"
  }
}

object UserVisitAction {
  /**
    * column name of the table
    */
  val columnNames = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")

  /**
    * Parse row data and return the object; if parsing fails return None
    *
    * @param line
    * @param splitSymbol
    * @return
    */
  def parseUserVisitAction(line: String, splitSymbol: String = ","): Option[UserVisitAction] = {
    val arr = line.split(splitSymbol)
    if (arr.length == 13) {
      Some(
        new UserVisitAction(
          arr(0),
          arr(1).toLong,
          arr(2),
          arr(3).toLong,
          arr(4).toLong,
          arr(5),
          arr(6),
          arr(7),
          arr(8),
          arr(9),
          arr(10),
          arr(11),
          arr(12).toInt
        )
      )
    } else None
  }
}


