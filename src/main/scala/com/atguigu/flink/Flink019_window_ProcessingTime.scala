package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink019_window_ProcessingTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 文件模拟数据流  一次性读取 有界流 可以写入
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // todo 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
    dataStream.print("dataStream")

    // todo 用keyBy + window +聚合 可以得到滚动窗口或者滑动窗口 比如得到最近一段时间内的结果 而不是从一开始到当前时间
    val resultStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(data => data._1)  // todo keyBy这里是对元组操作 所以不能用“id” 可以用0 也可以lambda表达式
//      .window(TumblingProcessingTimeWindows.of(  // todo 滚动 ProcessingTime的开窗方法  滚动
//        Time.seconds(10),  // window_size
//        Time.seconds(0) // offset
//      ))
      .window(SlidingProcessingTimeWindows.of( // todo 滑动ProcessingTime的开窗方法
        Time.seconds(10), // window_size
        Time.seconds(5), // sliding
        Time.seconds(0) // offset
      ))
      .reduce( // 滚动窗口为10s  返回每个sensor 滚动窗口之内的最小温度值
        (curState, newData) => {
          (curState._1, curState._2.min(newData._2))
        }
      )
    resultStream.print("timeWindow1")
    /*

     */


    env.execute("window_processingTime")


  }
}
