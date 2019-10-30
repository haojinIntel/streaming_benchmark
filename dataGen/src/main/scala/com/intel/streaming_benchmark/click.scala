package com.intel.streaming_benchmark

import java.util.Random
import java.util.concurrent.ThreadLocalRandom


object click {

//  val random = ThreadLocalRandom.current()
  val random = new Random
  val splitSymbol = ","
  // 用户数量
  val userNumbers = 1000
  // 用户每天访问的会话数量上限(100万)
  val userVisitSessionNumbers = 10000
  // 商品数量（100万）
  val productNumbers = 10000
  // 商品品类数量(50个)
  val productCategoryNumbers = 50
  // 职业
  val professionals = Array("程序员", "教师", "厨师", "司机", "医生", "护士", "设计师", "农民", "工人", "助理")
  val professionalTypeSize = professionals.length
  // 城市列表,格式为:(0,上海),(1,北京)....
  val citys: Array[(Int, String)] = Array("上海", "北京", "深圳", "广州", "南京", "杭州", "长沙", "南昌", "张家界", "香港", "澳门").zipWithIndex.map(_.swap)
  val cityTypeSize = citys.length
  // 性别列表
  val sexs = Array("male", "female", "unknown")
  val sexTypeSize = sexs.length
  // 搜索关键词
  val keywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
    "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
  val keywordSize = keywords.length

  var count = 0
}


case class ProductInfo(
                        productID: Long,
                        productName: String,
                        extendInfo: String
                      ) {
  /**
    * 格式化
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
    * 表列名称
    */
  val columnNames = Array("product_id", "product_name", "extend_info")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
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
    * 格式化时间
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
    * 表列明
    */
  val columnNames = Array("user_id", "user_name", "name", "age", "professional", "city", "sex")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
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
    * 格式化时间
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
    * 表列名称
    */
  val columnNames = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")

  /**
    * 解析行数据，并返回对象；如果解析失败返回None
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


