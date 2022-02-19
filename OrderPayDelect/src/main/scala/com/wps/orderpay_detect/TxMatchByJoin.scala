package com.wps.orderpay_detect

import com.wps.orderpay_detect.TxMatchDetect.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchByJoin {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId!="")
      .assignAscendingTimestamps(_.evenTime * 1000L)
      .keyBy(_.txId)

    //读取支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //join处理
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()

    env.execute("tx pay match by join job")
  }

}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1,in2))
  }
}
