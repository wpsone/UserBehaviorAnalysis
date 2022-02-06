package com.wps.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.net.URL

//定义输出Uv统计样例类
case class UvCount(windowEnd: Long,count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //从文件中读取数据
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类类型并提取时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream: DataStream[UvCount] = dataStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountResult())

    uvStream.print()

    env.execute("uv job")
  }
}

// 自定义实现全窗口函数，用一个Set结构来保存所有的userId，进行自动去重
class UvCountResult() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个Set
    var userIdSet: Set[Long] = Set[Long]()

    //遍历窗口中的所有数据，把userId添加到set中，自动去重
    for (userBehavior <- input) {
      userIdSet += userBehavior.userId
    }

    //将set的size作为去重后的uv值输出
    out.collect(UvCount(window.getEnd,userIdSet.size))
  }
}
