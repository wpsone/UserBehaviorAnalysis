package com.wps.marketanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.net.URL
import java.sql.Timestamp

//输入的广告点击事件样例类
case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)

//按照省份统计的输出结果样例类
case class CountByProvince(windowEnd:String,province:String,count:Long)

//输出黑名单报警信息
case class BlackListWarning(userId:Long,adId:Long,msg:String)

object AdStatisticsByGeo {
  val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取数据并转换成AdClickEvent
    val resource: URL = getClass.getResource("/AdClickLog.csv")
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //自定义process function,过滤大量刷点击的行为
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))


    //根据省份做分组，开窗聚合
    val adCountStream: DataStream[CountByProvince] = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag  ).print("blacklist")

    env.execute("ad statistics job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent] {
    //定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]) )
    //保存是否发送过黑名单的状态
    lazy val isSentBlackList:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))


    override def processElement(i: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      //取出count状态
      val curCount: Long = countState.value()
      //如果是第一次处理，注册定时器
      if (curCount==0) {
        val ts = (context.timerService().currentProcessingTime()/(1000*60*60*24)+1)*(1000*60*60*24)
        resetTime.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }

      //判断计数是否达到上限， 如果到达加入的黑名单
      if (curCount>=maxCount) {
        //判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          //输出到侧输出流
          context.output(blackListOutputTag,BlackListWarning(i.userId,i.adId,"Click over " + maxCount + " times today."))
        }
        return
      }
      //计数状态加1，输出数据到主流
      countState.update(curCount+1)
      collector.collect(i)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //定时器触发时，清空状态
      if (timestamp == resetTime.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTime.clear()
      }
    }
  }

}

class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class AdCountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}
