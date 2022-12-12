package com.atguigu.flink

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}


object Flink011_filter_FilterFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 文件模拟数据流
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt")) // todo 流式1行或多行读取

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // todo 3种方式实现filter
    val filterStream1: DataStream[SensorReading] = dataStream.filter(new MyFilter("sensor_1"))  // class实现
    val filterStream2: DataStream[SensorReading] = dataStream.filter(data => data.id.startsWith("sensor_1"))  // lambda实现
    // 函数实现成匿名类
    val filterStream3: DataStream[SensorReading] = dataStream.filter(
      new RichFilterFunction[SensorReading] { // todo 富函数  这里new FilterFunction[SensorReading] 效果是一样的
        override def filter(value: SensorReading): Boolean = {
          value.id.startsWith("sensor_1")
        }
      }
    )
//    filterStream1.print()
//    filterStream2.print()
    filterStream3.print()
    /*
    3> SensorReading(sensor_1,1547718199,35.8)
    7> SensorReading(sensor_1,1547718206,34.8)
    8> SensorReading(sensor_1,1547718208,31.8)
    10> SensorReading(sensor_1,1547718211,14.8)
    1> SensorReading(sensor_1,1547718216,15.8)
    3> SensorReading(sensor_1,1547718222,11.8)
    10> SensorReading(sensor_1,1547718234,13.4)
     */

    env.execute("filter_FilterFunction")
  }
}


class MyFilter(filterWord: String) extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith(filterWord)
  }
}
