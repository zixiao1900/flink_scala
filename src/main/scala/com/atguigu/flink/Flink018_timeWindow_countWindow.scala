package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink018_timeWindow_countWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) // todo 默认就是ProcessingTime 可以不设置这行

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
    // 一般DataStream -> keyBy -> window -> reduce/aggregate  todo 推荐这种
    // DataStream -> windowAll -> reduce/aggregate  todo 这样会把所有的数据发到一个分区再窗口分桶 没有并行了 损失性能
    // todo 用keyBy + timeWindow + 增量聚合函数/全窗口函数 可以得到滚动窗口或者滑动窗口 比如得到最近一段时间内的结果 而不是从一开始到当前时间
    // todo 增量聚合函数 每条数据到来就进行计算，保持一个简单的状态 ReduceFunction, AggregateFunction
    // todo 全窗口函数 先把窗口所有数据收集起来，等到计算的时候会遍历所有数据 ProcessWindowFunction
    val resultStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(data => data._1)  // todo keyBy这里是对元组操作 所以不能用“id” 可以用0 也可以lambda表达式
      .timeWindow(
        Time.seconds(10), // todo window_size
        Time.seconds(5) // todo sliding_size 如果没有sliding_size那就是滚动窗口
      )
//      .countWindow(2) // todo 滚动 计数窗口 传两个参数就是滑动  按key分组之后 e.g. 每两个sensor1 统计一次
      .reduce( // 滚动窗口为10s  返回每个sensor 滚动窗口之内的最小温度值
        (curState, newData) => {
          (curState._1, curState._2.min(newData._2))
        }
      )
    resultStream.print("timeWindow1")
    /*  TODO timeWindow滑动窗口
    dataStream> SensorReading(sensor_1,1547718199,35.8)
    dataStream> SensorReading(sensor_2,1547718201,15.4)
    dataStream> SensorReading(sensor_3,1547718202,6.7)
    dataStream> SensorReading(sensor_4,1547718205,38.1)
    timeWindow1> (sensor_1,35.8)  // todo 几乎是同一时间分组输出的 sensor_1, sensor_2, sensor_3, sensor_4之间的顺序不定
    timeWindow1> (sensor_2,15.4)
    timeWindow1> (sensor_4,38.1)
    timeWindow1> (sensor_3,6.7)
    dataStream> SensorReading(sensor_1,1547718206,34.8)
    dataStream> SensorReading(sensor_1,1547718208,31.8)
    dataStream> SensorReading(sensor_2,1547718209,21.8)
    timeWindow1> (sensor_3,6.7)
    timeWindow1> (sensor_4,38.1)
    timeWindow1> (sensor_1,31.8)
    timeWindow1> (sensor_2,15.4)
    dataStream> SensorReading(sensor_1,1547718211,14.8)
    dataStream> SensorReading(sensor_3,1547718212,24.7)
    dataStream> SensorReading(sensor_4,1547718214,24.9)
    dataStream> SensorReading(sensor_1,1547718216,15.8)
    timeWindow1> (sensor_2,21.8)
    timeWindow1> (sensor_1,14.8)
    timeWindow1> (sensor_3,24.7)
    timeWindow1> (sensor_4,24.9)
    dataStream> SensorReading(sensor_4,1547718218,17.8)
    dataStream> SensorReading(sensor_1,1547718222,11.8)
    timeWindow1> (sensor_3,24.7)
    timeWindow1> (sensor_4,17.8)
    timeWindow1> (sensor_1,11.8)
    dataStream> SensorReading(sensor_2,1547718223,14.8)
    dataStream> SensorReading(sensor_2,1547718224,14.9)
    dataStream> SensorReading(sensor_3,1547718226,15.7)
    dataStream> SensorReading(sensor_4,1547718231,19.8)
    dataStream> SensorReading(sensor_2,1547718232,32.1)
    timeWindow1> (sensor_4,17.8)
    timeWindow1> (sensor_1,11.8)
    timeWindow1> (sensor_2,14.8)
    timeWindow1> (sensor_3,15.7)
    dataStream> SensorReading(sensor_3,1547718233,33.2)
    dataStream> SensorReading(sensor_1,1547718234,13.4)
    timeWindow1> (sensor_4,19.8)
    timeWindow1> (sensor_1,13.4)
    timeWindow1> (sensor_2,14.8)
    timeWindow1> (sensor_3,15.7)
    timeWindow1> (sensor_3,33.2)
    timeWindow1> (sensor_1,13.4)
     */

    /*  todo 滚动计数窗口 countWindow
    dataStream> SensorReading(sensor_1,1547718199,35.8)
    dataStream> SensorReading(sensor_2,1547718201,15.4)
    dataStream> SensorReading(sensor_3,1547718202,6.7)
    dataStream> SensorReading(sensor_4,1547718205,38.1)
    dataStream> SensorReading(sensor_1,1547718206,34.8)
    dataStream> SensorReading(sensor_1,1547718208,31.8)
    timeWindow1> (sensor_1,34.8)
    dataStream> SensorReading(sensor_2,1547718209,21.8)
    dataStream> SensorReading(sensor_1,1547718211,14.8)
    timeWindow1> (sensor_2,15.4)
    timeWindow1> (sensor_1,14.8)
    dataStream> SensorReading(sensor_3,1547718212,24.7)
    dataStream> SensorReading(sensor_4,1547718214,24.9)
    timeWindow1> (sensor_3,6.7)
    timeWindow1> (sensor_4,24.9)
    dataStream> SensorReading(sensor_1,1547718216,15.8)
    dataStream> SensorReading(sensor_4,1547718218,17.8)
    dataStream> SensorReading(sensor_1,1547718222,11.8)
    timeWindow1> (sensor_1,11.8)
    dataStream> SensorReading(sensor_2,1547718223,14.8)
    dataStream> SensorReading(sensor_2,1547718224,14.9)
    timeWindow1> (sensor_2,14.8)
    dataStream> SensorReading(sensor_3,1547718226,15.7)
    dataStream> SensorReading(sensor_4,1547718231,19.8)
    dataStream> SensorReading(sensor_2,1547718232,32.1)
    timeWindow1> (sensor_4,17.8)
    dataStream> SensorReading(sensor_3,1547718233,33.2)
    timeWindow1> (sensor_3,15.7)
    dataStream> SensorReading(sensor_1,1547718234,13.4)
     */


    env.execute("timeWindow")


  }
}
