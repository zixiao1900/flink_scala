package com.atguigu.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object Flink003_source_fromCollections_fromElements_readTextFile {
  def main(args: Array[String]): Unit = {
    // 流任务环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // todo 从内存中或者文件中 读取一个有界流

    // todo 1. 从集合(内存)中读取数据 fromCollection
    val dataList: List[SensorReading] = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1),
    )

    val streamFromCollection: DataStream[SensorReading] = env.fromCollection(dataList)
    streamFromCollection.print()
    /*  有并行度  顺序不能保证
    12> SensorReading(sensor_10,1547718205,38.1)
    9> SensorReading(sensor_1,1547718199,35.8)
    10> SensorReading(sensor_6,1547718201,15.4)
    11> SensorReading(sensor_7,1547718202,6.7)
     */

    // todo 2. fromElements
    val streamFromElements: DataStream[Any] = env.fromElements(1.0, 35, "hello")
    streamFromElements.print()
    /* 乱序
    7> hello
    5> 1.0
    6> 35
     */

    // todo 3. 从文件中读取
    val StreamFromFile: DataStream[String] = env.readTextFile("datas/sensor.txt")
    StreamFromFile.print()
    /* 乱序
    2> sensor_7,1547718202,6.7
    9> sensor_1,1547718199,35.8
    11> sensor_6,1547718201,15.4
    5> sensor_10,1547718205,38.1
     */

    // todo 流处理执行
    env.execute("sourceFromCollection")
  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}


