package com.intel.streaming_benchmark

import java.io.FileWriter
import java.net.InetAddress
import java.util.{Properties, UUID}

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
    * 产生用户信息的模拟代码
    *
    * @param
    * @return
    */
  def mockUserInfo() = {
    val producer = createProducer
    // val buffer = ArrayBuffer[dataUtil.UserInfo]()
    for (i <- 0 until userNumbers) {
//      val userId = i.toLong
//      val userName = s"user_${i}"
//      val name = s"name_${i}"
//      val age = random.nextInt(60)
//      val professional = professionals(random.nextInt(professionalTypeSize))
//      val city = citys(random.nextInt(cityTypeSize))._2
//      val sex = sexs(random.nextInt(sexTypeSize))
      val userId = i.toLong
      val age = (total % 60).toInt
      val userName = s"user_${i}"
      val name = s"name_${i}"
      val professional = professionals((total % professionalTypeSize).toInt)
      val city = citys((total%cityTypeSize).toInt)._2
      val sex = sexs((total % sexTypeSize).toInt)
      //      println(dataUtil.UserInfo(userId, userName, name,age, professional, city, sex).formatted(","))
      producer.send(new ProducerRecord("userInfo", UserInfo(
        userId, userName, name,
        age, professional, city, sex).formatted(",").getBytes()))
    }
  }

  /**
    * 模拟产生UserVisitAction的访问数据
    *
    * @param
    * @return
    */

  def mockUserVisitAction(time: Long) = {
    val date: String = DateUtils.getTodayDate()
    val producer = createProducer
    val start: Long = System.currentTimeMillis()

    // 根据上一个操作的时间获取当前操作的时间
    def getCurrentActionTime(preActionTime: Long): Long = {
      // 90%，增加1分钟以内，10%增加1-5分钟
//      val (least, bound) = if (random.nextDouble() <= 0.9) {
//        (1, 6)
//      } else {
//        (6, 30)
//      }
//      preActionTime + random.nextInt(bound)
      preActionTime + total % 60
    }

    // 产生一个商品id和商品品类
    def generateProduceAndCategoryId(): (Long, Long) = {

//      val produceID = random.nextInt(productNumbers)
      val produceID = total % productNumbers
        (produceID, produceID % click.productCategoryNumbers)
    }

    // 产生pageView数据
    def generatePageView(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // 添加新数据
        // pageView的页面ID范围是:[0,100)
//        val pageId: Long = random.nextInt(100)
        val pageId: Long = total % 100
        val actionTime: Long = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""



        // 添加数据
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        length = length + message.length
        total = total + 1
        // 进入下一步操作
        /**
          * 浏览之后可能存在搜索、点击和继续浏览三种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，45%继续浏览，25%搜索，20%的点击，10%直接退出
          * 当times次数[3,10)的时候，10%浏览，25%搜素，35%的点击，30%直接退出
          * 当times次数[10,20)的时候，5%的浏览，5%的搜索，10%的点击，80%直接退出
          **/
//        val randomValue = random.nextDouble()
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
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp  <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, 结束
        }

//        generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        if(total % 5 == 0){
//          // 浏览
//          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 3 == 0){
//          // 搜索
//          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 2 == 0){
//          // 点击
//          generateClick(times + 1, userId, sessionId, cityId, actionTime)
//        }else{
//          // nothings, 结束
//        }



      }
    }

    // 产生搜索数据
    def generateSearch(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // 添加新数据
        // search的页面ID范围是:[100,150)
 //       val pageId: Long = random.nextInt(50) + 100
        val pageId: Long = total % 50 + 100
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = keywords((total % keywordSize).toInt)
        //       val searchKeyword: String = keywords(random.nextInt(keywordSize))
        val clickCategoryId: String = ""
        val clickProductId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit",message))
        length = length + message.length
        total = total + 1
        // 进入下一步操作
        /**
          * 搜索之后可能存在点击、浏览和继续搜索三种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，20%浏览，25%搜索，45%的点击，10%直接退出
          * 当times次数[3,10)的时候，10%浏览，10%搜素，30%的点击，50%直接退出
          * 当times次数[10,20)的时候，1%的浏览，1%的搜索，8%的点击，90%直接退出
          **/
//        val randomValue = random.nextDouble()
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
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, 结束
        }

//        if(total % 5 == 0){
//          // 浏览
//          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 3 == 0){
//          // 搜索
//          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 2 == 0){
//          // 点击
//          generateClick(times + 1, userId, sessionId, cityId, actionTime)
//        }else{
//          // nothings, 结束
//        }




      }
    }

    // 产生点击事件数据
    def generateClick(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // click的页面ID范围是:[150,300)
//        val pageId: Long = random.nextInt(150) + 150
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

        // 添加数据
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        // 进入下一步操作
        //        count =count + 1
        total = total + 1
        length = length + message.length
        /**
          * 点击之后可能存在浏览、搜索、下单和继续点击四种情况， 也存在直接退出的情况
          * 当times次数小于3的时候，10%继续浏览，10%搜索，50%下单，25%的点击，5%直接退出
          * 当times次数[3,10)的时候，5%继续浏览，5%搜索，45%下单，20%的点击，25%直接退出
          * 当times次数[10,20)的时候，1%继续浏览，1%搜索，30%下单，8%的点击，60%直接退出
          **/
//        val randomValue = random.nextDouble()
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
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // 下单
          generateOrder(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t4) {
          // 点击
          generateClick(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, 结束
        }
//        if(total % 5 == 0){
//          // 浏览
//          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 3 == 0){
//          // 搜索
//          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 2 == 0){
//          // 点击
//          generateClick(times + 1, userId, sessionId, cityId, actionTime)
//        }else{
//          // nothings, 结束
//        }





      }
    }

    // 产生订单数据
    def generateOrder(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long): Unit = {
      if (times < 20) {
        // order的页面ID范围是:[300,301)
        val pageId: Long = 300
        val actionTime = getCurrentActionTime(preActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        // 可能存在多个商品或者品类在一起下单,数量范围:[1,6)
        val randomProductNumbers = total % 5 + 1
        //val randomProductNumbers = random.nextInt(1) + 5
        val bf = ArrayBuffer[(Long, Long)]()
        for (j <- 0 until randomProductNumbers.toInt) {
          bf += generateProduceAndCategoryId()
        }
        // 去掉重复数据
        val nbf = bf.distinct

        val orderCategoryIds: String = nbf.map(_._2).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val orderProductIds: String = nbf.map(_._1).mkString(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR)
        val payCategoryIds: String = ""
        val payProductIds: String = ""

        // 添加数据
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))
        total = total + 1
        length = length + message.length
        // 进入下一步操作
        /**
          * 下单之后可能存在搜索、浏览和支付三种情况， 也存在直接退出的情况
          * 当times次数小于等于3的时候，5%继续浏览，5%搜索，90%支付，0%直接退出
          * 当times次数(3,10)的时候，5%继续浏览，5%搜索，80%支付，10%直接退出
          * 当times次数[10,20)的时候，1%继续浏览，1%搜索，70%支付，28%直接退出
          **/
//        val randomValue = random.nextDouble()
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
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t3) {
          // 支付
          generatePay(times + 1, userId, sessionId, cityId, actionTime, productIds = orderProductIds, categoryIds = orderCategoryIds)
        } else {
          // nothings, 结束
        }

//        if(total % 5 == 0){
//          // 浏览
//          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 3 == 0){
//          // 搜索
//          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 2 == 0){
//          // 点击
//          generateClick(times + 1, userId, sessionId, cityId, actionTime)
//        }else{
//          // nothings, 结束
//        }

      }
    }

    // 产生支付数据
    def generatePay(times: Int, userId: Long, sessionId: String, cityId: Int, preActionTime: Long, productIds: String, categoryIds: String): Unit = {
      if (times <= 20) {
        // pay的页面ID范围是:301
        val pageId: Long = 301
        val actionTime = getCurrentActionTime(preActionTime)
        //        val actionTime: String = DateUtils.parseLong2String(currentActionTime)
        val searchKeyword: String = ""
        val clickProductId: String = ""
        val clickCategoryId: String = ""
        val orderCategoryIds: String = ""
        val orderProductIds: String = ""
        val payCategoryIds: String = categoryIds
        val payProductIds: String = productIds

        // 添加数据
        val message = UserVisitAction(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId).formatted(",").getBytes()
        producer.send(new ProducerRecord("userVisit", message))

        total = total + 1
        length = length + message.length
        // 进入下一步操作
        /**
          * 支付之后可能存在搜索和浏览两种情况， 也存在直接退出的情况
          * 当times次数[0,10)的时候，45%继续浏览，45%搜索，10%直接退出
          * 当times次数[10,20)的时候，20%继续浏览，20%搜索，60%直接退出
          **/
//        val randomValue = random.nextDouble()
        val (t1, t2) =
          if (times < 10) {
            (4, 8)
          } else {
            (1, 3)
          }

        val tmp = seed % 10
        seed = seed + 1

        if (tmp <= t1) {
          // 浏览
          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
        } else if (tmp <= t2) {
          // 搜索
          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
        } else {
          // nothings, 结束
        }

//        if(total % 5 == 0){
//          // 浏览
//          generatePageView(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 3 == 0){
//          // 搜索
//          generateSearch(times + 1, userId, sessionId, cityId, actionTime)
//        } else if (total % 2 == 0){
//          // 点击
//          generateClick(times + 1, userId, sessionId, cityId, actionTime)
//        }else{
//          // nothings, 结束
//        }

      }
    }

    //    val sessionNumbers = random.nextInt(userVisitSessionNumbers / 10, userVisitSessionNumbers)
    //  val sessionNumbers = userVisitSessionNumbers
    var flag: Boolean = true
    while (flag) {
      val startTime = System.currentTimeMillis()
      val userId: Long = random.nextInt(userNumbers)
//      val userId: Long = total % userNumbers
      val sessionId = hostName + "_" + threadName + "_"+ total
      //val cityId: Int = citys(random.nextInt(cityTypeSize))._1
      val cityId = citys((total % cityTypeSize).toInt)._1
      seed = random.nextInt(100)
      // action主要分为：浏览、搜索、点击、下单及支付
      /**
        * 假设访问链有以下情况：
        * 1. 浏览 -> 搜索 -> 点击 -> 下单 -> 支付
        * 2. 搜索 -> 点击 -> 下单 -> 支付
        * 3. 浏览 -> 点击 -> 下单 -> 支付
        * 注意：其中浏览、搜索、点击可能连续出现多次，但是下单和支付不会存在连续出现的情况
        * 当一个流程执行到下单或者支付的时候，可能会再次重新开始执行
        * 假设一个会话中的事件触发次数最多不超过20次
        * 每两个事件之间的时间间隔不超过5分钟，最少1秒钟
        * ======>
        * 浏览之后可能存在搜索、点击和继续浏览三种情况
        * 搜索之后可能存在点击、浏览和继续搜索三种情况
        * 点击之后可能存在浏览、搜索、下单和继续点击四种情况
        * 下单之后可能存在搜索、浏览和支付三种情况
        * 支付之后可能存在搜索和浏览两种情况
        * 备注：所有事件之后都可能存在结束的操作
        **/
//      // 80%的几率进入浏览，20%直接进入搜索
//      if (random.nextDouble() <= 0.8) {
//        // 产生一个浏览数据
//        generatePageView(0, userId, sessionId, cityId, startTime)
//      } else {
//        // 产生一个搜索数据
//        generateSearch(0, userId, sessionId, cityId, startTime)
//      }

      // 80%的几率进入浏览，20%直接进入搜索
      if (total % 5 < 4) {
        // 产生一个浏览数据
        generatePageView(0, userId, sessionId, cityId, startTime)
      } else {
        // 产生一个搜索数据
        generateSearch(0, userId, sessionId, cityId, startTime)
      }

      if ( (System.currentTimeMillis() - start) > time*1000) {
        flag = false
        //   System.out.println(Thread.currentThread().getName + "Topic3  Runtime: " + time + " Count:" + total + "; Total data: " + length + "B" )
        val out = new FileWriter("/home/streaming_benchmark/result/kafkaProducer.log",true)
        out.write(Thread.currentThread().getName + " Topic3  Runtime: " + time + " Count:" + total + "; Total data: " + length + "B" + "\r\n")
        out.close()
      }

    }
  }

  /**
    * 模拟产生商品数据
    *
    * @param
    * @return
    */
  def mockProductInfo() = {
    // 1. 创建Data数据
    val producer = createProducer
    val buffer = ArrayBuffer[ProductInfo]()
    for (i <- 0 until productNumbers) {
      val productID: Long = i.toLong
      val productName: String = s"product_${productID}"
      // 60%是第三方商品 40%是自营商品
      val extendInfo: String = {
        val obj = new JSONObject()
        if (random.nextDouble() <= 0.4) {
          // 自营商品
          obj.put("product_type", "0")
        } else {
          // 第三方商品
          obj.put("product_type", "1")
        }
        obj.toJSONString
      }
      producer.send(new ProducerRecord("productInfo", ProductInfo(productID, productName, extendInfo).formatted(",").getBytes()))

    }

  }

}
