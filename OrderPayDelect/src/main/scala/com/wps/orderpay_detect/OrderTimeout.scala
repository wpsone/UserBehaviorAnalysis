package com.wps.orderpay_detect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL
import java.util

//定义输入订单事件的样例类
case class OrderEvent(orderId:Long,eventType:String,txId:String,evenTime:Long)
//定义输出结果样例类
case class OrderResult(orderId:Long,resultMsg:String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/OrderLog.csv")
//    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
      val dataArray: Array[String] = data.split(",")
      OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong
      )
    })
    .assignAscendingTimestamps(_.evenTime * 1000L)
    .keyBy(_.orderId)

    //2.定义一个匹配模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //3.把模式应用到stream上，得到一个pattern stream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    //4.调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag  ).print("timeout")

    env.execute("order timeout job")
  }
}

//自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout")
  }
}

//自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId,"payed successfully")
  }
}