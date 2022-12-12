package com.atguigu.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

object Flink009_split_select {
  def main(args: Array[String]): Unit = {
    // todo split   DataStream -> SplitStream: 根据某些特征把一个DataStream拆分成两个或者多个DataStream
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt")) // todo 流式1行或多行读取

    // 转为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // todo 需求 传感器数据按照温度高低(以30度为界) 拆分成两个流
    // todo 分流操作 split + select
    val splitStream: SplitStream[SensorReading] = dataStream
      .split(data => {
        if (data.temperature > 30.0) {
          List("high")
        }
        else {
          List("low")
        }
      })

    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    // todo 可以选多个分流的
    val allStream: DataStream[SensorReading] = splitStream.select("high", "low")
    highStream.print("high")
    lowStream.print("low")
//    allStream.print("all")
    /*  todo  分流操作
    high:6> SensorReading(sensor_1,1547718199,35.8)
    low:7> SensorReading(sensor_2,1547718201,15.4)
    low:8> SensorReading(sensor_3,1547718202,6.7)
    high:10> SensorReading(sensor_1,1547718206,34.8)
    high:9> SensorReading(sensor_4,1547718205,38.1)
    high:11> SensorReading(sensor_1,1547718208,31.8)
    low:1> SensorReading(sensor_1,1547718211,14.8)
    low:12> SensorReading(sensor_2,1547718209,21.8)
    low:2> SensorReading(sensor_3,1547718212,24.7)
    low:3> SensorReading(sensor_4,1547718214,24.9)
    low:4> SensorReading(sensor_1,1547718216,15.8)
    low:5> SensorReading(sensor_4,1547718218,17.8)
    low:6> SensorReading(sensor_1,1547718222,11.8)
    low:7> SensorReading(sensor_2,1547718223,14.8)
    low:8> SensorReading(sensor_2,1547718224,14.9)
    low:9> SensorReading(sensor_3,1547718226,15.7)
    low:10> SensorReading(sensor_4,1547718231,19.8)
    high:11> SensorReading(sensor_2,1547718232,32.1)
    high:12> SensorReading(sensor_3,1547718233,33.2)
    low:1> SensorReading(sensor_1,1547718234,13.4)

     */
    env.execute("split_select")

  }
}
