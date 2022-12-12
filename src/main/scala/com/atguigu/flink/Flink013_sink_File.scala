package com.atguigu.flink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}



object Flink013_sink_File {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // 文件模拟数据流
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt")) // todo 流式1行或多行读取  writeAsCsv无法写入

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
    dataStream.print()

    // todo 简单保存方式  只能对一次性读取的文件写入 不能对流式的数据进行写入
    dataStream.writeAsCsv("outputs/sensor_writeAsCsv.csv")
    /*  todo 会自动将SensorReading的属性按，分隔
    sensor_1,1547718199,35.8
     */

    // todo 通用写入方式   既可以写入一次性读取的DataStream  可以写入流式读取的DataStream
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("outputs/addSink_sensor1_stream"),  // todo 保存路径
        new SimpleStringEncoder[SensorReading]() // todo 编码方式
      ).build()
    )
    /*  todo 写入的就是下面这样的字符串
    SensorReading(sensor_4,1547718218,17.8)
     */


    env.execute("sink_File")
  }
}
