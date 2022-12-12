package com.atguigu.flink

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}



object Flink012_map_MapFunction_RichMapFunction {
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
    // todo 3种方式实现map 当前效果一样
    // todo lambda
    val mapStream1: DataStream[String] = dataStream.map(data => data.id + " temperature")
//    mapStream1.print()

    // todo MapFunction[In泛型, Out泛型]
    val mapStream2: DataStream[String] = dataStream.map(new MyMap)
//    mapStream2.print()

    // todo RichMapFunction[In泛型, Out泛型]
    val mapStream3: DataStream[String] = dataStream.map(new MyRichMap)
    mapStream3.print()
    /*
    5> sensor_1 temperature
    6> sensor_2 temperature
    7> sensor_3 temperature
    8> sensor_4 temperature
    9> sensor_1 temperature
    10> sensor_1 temperature
    11> sensor_2 temperature
    12> sensor_1 temperature
    1> sensor_3 temperature
    3> sensor_1 temperature
    2> sensor_4 temperature
    4> sensor_4 temperature
    5> sensor_1 temperature
    6> sensor_2 temperature
    7> sensor_2 temperature
    8> sensor_3 temperature
    9> sensor_4 temperature
    10> sensor_2 temperature
    11> sensor_3 temperature
    12> sensor_1 temperature
     */


    env.execute("map_MapFunction_RichMapFunction")
  }
}

class MyMap extends MapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = {
    value.id + " temperature"
  }
}


// todo 富函数 可以获取运行时上下文和生命周期
class MyRichMap extends RichMapFunction[SensorReading, String]{
  override def map(value: SensorReading): String = {
    value.id + " temperature"
  }

  override def open(parameters: Configuration): Unit = {
    // todo 生命周期里面做一些初始化操作  比如数据库连接
  }


  override def close(): Unit = {
    // todo 一般做收尾工作，比如关闭连接，或者清空状态
  }
}