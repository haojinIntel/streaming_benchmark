package com.intel.streaming_benchmark

import java.net.InetAddress
import java.util.Properties
import com.alibaba.fastjson.JSONObject
import com.intel.streaming_benchmark.click.{cityTypeSize, citys, keywordSize, keywords, productNumbers, professionalTypeSize, professionals, random, sexTypeSize, sexs, userNumbers}
import com.intel.streaming_benchmark.common.{ConfigLoader, DateUtils, StreamBenchConfig}
import com.intel.streaming_benchmark.utils.Constants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

class ClickProducer(val time:Long, val cl: ConfigLoader){
  var total = 0L
  var length = 0L
  var threadName = Thread.currentThread().getName
  var hostName = InetAddress.getLocalHost.getHostName
  var seed = 0
  def run(): Unit = {
    //  mockUserInfo()
    //  mockProductInfo
    mockUserVisitAction(time)

  }

  private def createProducer = {
    val properties = new Properties
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST))
    new KafkaProducer[Array[Byte], Array[Byte]](properties)
  }


  /**
    * Simulation code for generating user information
    *
    * @param
    * @return
    */
  def mockUserInfo() = {
    val producer = createProducer
    for (i <- 0 until userNumbers) {
      val userId = i.toLong
      val age = (total % 60).toInt
      val userName = s"user_${i}"
      val name = s"name_${i}"
      val professional = professionals((total % professionalTypeSize).toInt)
      val city = citys((total%cityTypeSize).toInt)._2
      val sex = sexs((total % sexTypeSize).toInt)
      producer.send(new ProducerRecord("userInfo", UserInfo(
        userId, userName, name,
        age, professional, city, sex).formatted(",").getBytes()))
    }
  }

  /**
    * Simulation code for generating data of userVisitAction
    *
    * @param
    * @return
    */

  def mockUserVisitAction(time: Long) = {
    val date: String = DateUtils.getTodayDate()
    val producer = createProducer
    val start: Long = System.currentTimeMillis()

    // get action time according the time of last action
    def getCurrentActionTime(preActionTime: Long): Long = {
      preActionTime + total % 60
    }

    // generate a produceID and productCategoryNumber
    def generateProduceAndCategoryId(): (Long, Long) = {
      val produceID = total % productNumbers
        (produceID, produceID % click.productCategoryNumbers)
    }

    // generate date for pageView
    def generatePageView(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // pageView ID:[0,100)
        val pageId: Long = total % 100
        val actionTime: Long = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // Add data
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        length = length + message.length
        total = total + 1
        // Go to next action
        val (t1, t2, t3) =
          if (times < 3) {
            (4, 7, 9)
          } else if (times < 10) {
            (2, 4, 7)
          } else {
            (1, 2, 3)
          }
        val tmp = seed % 10
        seed = seed + 1
        if (tmp  <= t1) {
          // Visit
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp  <= t2) {
          // Search
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // Click
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, finish
        }

      }
    }

    // Generate data for searching
    def generateSearch(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // search ID:[100,150)
        val pageId: Long = total % 50 + 100
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = keywords((total % keywordSize).toInt)
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // Add data
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit",message))
        length = length + message.length
        total = total + 1
        // Go to next action
        val (t1, t2, t3) =
          if (times < 3) {
            (2, 5, 8)
          } else if (times < 10) {
            (1, 2, 5)
          } else {
            (1, 2, 3)
          }
        val tmp = seed % 10
        seed = seed + 1
        if (tmp <= t1) {
          // Visit
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // Search
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // Click
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, finish
        }
      }
    }

    // Generate data for clicking
    def generateClick(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // click ID:[150,300)
        val pageId: Long = total % 150 + 150
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val (productID, categoryID) = generateProduceAndCategoryId()
        val clickProductId: String = productID.toString
        val clickCategoryId: String = categoryID.toString
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // Add data
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        //  Go to next action
        total = total + 1
        length = length + message.length

        val (t1, t2, t3, t4) =
          if (times < 3) {
            (3, 6, 15, 18)
          } else if (times < 10) {
            (2, 4, 11, 15)
          } else {
            (1, 2, 6, 8)
          }

        val tmp = seed % 20
        seed = seed + 1
        if (tmp <= t1) {
          // Visit
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // Search
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // Order
          generateOrder(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t4) {
          // Click
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, finish
        }

      }
    }

    // Generate date for order
    def generateOrder(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // order ID:[300,301)
        val pageId: Long = 300
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        // There may be some product ordered together, range:[1,6)
        val randomProductNumbers = total % 5 + 1
        val bf = ArrayBuffer[(Long, Long)]()
        for (j <- 0 until randomProductNumbers.toInt) {
          bf += generateProduceAndCategoryId()
        }
        val nbf = bf.distinct

        val orderCategoryIds: String = nbf.map(_._2).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val orderProductIds: String = nbf.map(_._1).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // Add data
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        total = total + 1
        length = length + message.length
        // Go to next action
        val (t1, t2, t3) =
          if (times <= 3) {
            (1, 2, 9)
          } else if (times < 10) {
            (1, 2, 8)
          } else {
            (1, 2, 7)
          }

        val tmp = seed % 10
        seed = seed + 1

        if (tmp <= t1) {
          // Visit
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // Search
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // Pay
          generatePay(times + 1, userId, sessionId, cityId, actionTime, productIds = orderProductIds, categoryIds = orderCategoryIds)
        } else {
          // nothings, finish
        }

      }
    }

    // Generate data for pay
    def generatePay(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long, productIds: String, categoryIds: String): Unit = {
      if (times <= 20) {
        // pay ID:301
        val pageId: Long = 301
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = categoryIds
        val payProductIds: String = productIds

        // Add data
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))

        total = total + 1
        length = length + message.length
        // Go to next action
        val (t1, t2) =
          if (times < 10) {
            (4, 8)
          } else {
            (1, 3)
          }

        val tmp = seed % 10
        seed = seed + 1

        if (tmp <= t1) {
          // Visit
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // Search
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, finish
        }

      }
    }

    var flag: Boolean = true
    while (flag) {
      val startTime = System.currentTimeMillis()
      val userId: Long = random.nextInt(userNumbers)
      val sessionId = hostName + "_" + threadName + "_"+ total
      val cityId = citys((total % cityTypeSize).toInt)._1
      seed = random.nextInt(100)
      // action主要分为：浏览、搜索、点击、下单及支付
      /**
        * Suppose the access chain has several situations:
        * 1. Visit -> Search-> Click -> Order -> Pay
        * 2. Search -> Click -> Order -> Pay
        * 3. Visit -> Click -> Order -> Pay
        * Note：Visit, Search, Click can be generated continuously while Pay and Order can not appear successfully
        * ======>
        * After visiting, there may be search, click and visit action.
        * After searching, there may be click, search and search action.
        * After clicking, there may be visit, search, order and click action.
        * After ordering, there may be search, visit and pay action.
        * After paying, there may be search and visit action支付之后可能存在搜索和浏览两种情况
        * Note：After all action, there may be finish action.
        **/

      // 80% visit, 20% click
      if (total % 5 < 4) {
        // generate data for visit
        generatePageView(0, userId, sessionId, cityId, startTime)
      } else {
        // generate data for search
        generateSearch(0, userId, sessionId, cityId, startTime)
      }

      if ( (System.currentTimeMillis() - start) > time*1000) {
        flag = false
      }

    }
  }

  /**
    * Simulation code for generating product
    *
    * @param
    * @return
    */
  def mockProductInfo() = {
    val producer = createProducer
    val buffer = ArrayBuffer[ProductInfo]()
    for (i <- 0 until productNumbers) {
      val productID: Long = i.toLong
      val productName: String = s"product_${productID}"
      // 60% third party products; 40% proprietary products
      val extendInfo: String = {
        val obj = new JSONObject()
        if (random.nextDouble() <= 0.4) {
          // proprietary product
          obj.put("product_type", "0")
        } else {
          // third party products
          obj.put("product_type", "1")
        }
        obj.toJSONString
      }
      producer.send(new ProducerRecord("productInfo", ProductInfo(productID, productName, extendInfo).formatted(",").getBytes()))

    }
  }

}
