package com.wps.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.{lang, util}
import java.net.URL
import scala.collection.mutable.ListBuffer

//输入的登录事件样例类
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
//输出异常报警信息样例类
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

    val warningStream: DataStream[Warning] = loginEventStream.keyBy(_.userId) //以用户id做分组
      .process(new LoginWarning(2))

    warningStream.print()

    env.execute("login fail detect job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning] {
  //定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
//    val loginFailList: lang.Iterable[LoginEvent] = loginFailState.get()
//    //判断类型是否是fail，只添加fail的事件到状态
//    if (i.eventType=="fail") {
//      if (!loginFailList.iterator().hasNext) {
//        context.timerService().registerEventTimeTimer(i.eventTime*1000L+2000L)
//      }
//      loginFailState.add(i)
//    } else {
//      //如果成功，清空状态
//      loginFailState.clear()
//    }

    if (i.eventType=="fail") {
      //如果是失败，判断之前是否有登录失败事件
      val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
      if (iter.hasNext) {
        //如果已经有登录失败事件，就比较事件时间
        val firstFail: LoginEvent = iter.next()
        if (i.eventTime < firstFail.eventTime + 2) {
          //如果两次间隔小于2秒，输出报警
          collector.collect(Warning(i.userId,firstFail.eventTime,i.eventTime,"login fail in 2 seconds."))
        }
        //跟新最近一次登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(i)
      } else {
        //如果是第一次登录失败，直接添加到状态
        loginFailState.add(i)
      }
    } else {
      //如果成功，清空状态
      loginFailState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    //触发定时器时候，根据状态失败个数决定是否输出报警
//    val allLoginFails = new ListBuffer[LoginEvent]
//    val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
//    while (iter.hasNext) {
//      allLoginFails+=iter.next()
//    }
//
//    //判断个数
//    if (allLoginFails.length>=maxFailTimes) {
//      out.collect(Warning(allLoginFails.head.userId,allLoginFails.head.eventTime,allLoginFails.last.eventTime,"login fail in 2 seconds for " + allLoginFails.length + " times."))
//    }
//    //清空状态
//    loginFailState.clear()
//  }
}
