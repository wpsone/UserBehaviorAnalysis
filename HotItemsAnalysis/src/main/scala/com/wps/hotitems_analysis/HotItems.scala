package com.wps.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.Properties
import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {

  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 定义事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","node01:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

//    val dataStream: DataStream[UserBehavior] = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
val dataStream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
  .map(data => {
    val dataArray: Array[String] = data.split(",")
    UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
  })
  .assignAscendingTimestamps(_.timestamp * 100L)

    //3.transform 处理数据
    val processedStream: DataStream[String] = dataStream.filter(_.behavior == "pv")
      //      .keyBy("itemId")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResult())
      //按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    //4.sink：控制台输出
//    dataStream.print()
    processedStream.print()

    env.execute("hot items job")
  }


}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

//自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double] {
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp,acc._2+1)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1,acc._2+acc1._2)
}

//自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

//自定义的处理函数
class TopNHotItems(topSize : Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]) )
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit ={
    //把每条数据存入状态列表
    itemState.add(i)
    //注册一个定时器
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }

  //定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit ={
    //将所有state中的数据取出，放到一个List Buffer中
    val allItems:ListBuffer[ItemViewCount] = new ListBuffer()

    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems+=item
    }
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemState.clear()

    //将排名结果格式化输出
    val result = new StringBuilder()
    result.append("时间：" ).append(new Timestamp(timestamp-1    )).append("\n")
    //输出每个商品的信息
    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No"  ).append(i+1  ).append(":")
        .append(" 商品ID="  ).append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("=======================================")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}