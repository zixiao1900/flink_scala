package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink007_KeyBy_min_max_sum_minBy_maxBy {
  def main(args: Array[String]): Unit = {
    // todo KeyBy  DataStream -> KeyedStream 逻辑的将一个流拆分成不相交的分区，相同key的元素一定在一个分区中 一个分区 可能有多个key的元素
    // todo keyBy中传字段名或者idx都可以
    // todo 简单的聚合算子 min max sum minBy maxBy
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt")) // todo 流式1行或多行读取

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    val keyStream: KeyedStream[SensorReading, Tuple] = dataStream.keyBy("id")
//    keyStream.print("keyStream")
    /*  todo 只做keyBy没有做聚合之前 打印结果和原来dataStream相同
    keyStream:7> SensorReading(sensor_1,1547718199,35.8)
    keyStream:3> SensorReading(sensor_2,1547718201,15.4)
    keyStream:1> SensorReading(sensor_3,1547718202,6.7)
    keyStream:7> SensorReading(sensor_1,1547718206,34.8)
    keyStream:10> SensorReading(sensor_4,1547718205,38.1)
    keyStream:7> SensorReading(sensor_1,1547718208,31.8)
    keyStream:3> SensorReading(sensor_2,1547718209,21.8)
    keyStream:7> SensorReading(sensor_1,1547718211,14.8)
    keyStream:1> SensorReading(sensor_3,1547718212,24.7)
    keyStream:7> SensorReading(sensor_1,1547718216,15.8)
    keyStream:10> SensorReading(sensor_4,1547718214,24.9)
    keyStream:10> SensorReading(sensor_4,1547718218,17.8)
    keyStream:3> SensorReading(sensor_2,1547718223,14.8)
    keyStream:7> SensorReading(sensor_1,1547718222,11.8)
    keyStream:1> SensorReading(sensor_3,1547718226,15.7)
    keyStream:3> SensorReading(sensor_2,1547718224,14.9)
    keyStream:3> SensorReading(sensor_2,1547718232,32.1)
    keyStream:10> SensorReading(sensor_4,1547718231,19.8)
    keyStream:1> SensorReading(sensor_3,1547718233,33.2)
    keyStream:7> SensorReading(sensor_1,1547718234,13.4)

     */
    // todo min 根据id进行分组  按sensor_id分组 返回目前位置 id对应的第一个出现的timeStamp 和最小的temperature
    val aggStreamMin: DataStream[SensorReading] = dataStream
      .keyBy("id") // todo keyBy有多种方式 字段名(样例类)， 索引， lambda表达式(元组)
      .min("temperature")

//    aggStreamMin.print()
    /*
    3> SensorReading(sensor_2,1547718201,15.4)
    7> SensorReading(sensor_1,1547718199,35.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,34.8)
    7> SensorReading(sensor_1,1547718199,31.8)
    3> SensorReading(sensor_2,1547718201,15.4)
    7> SensorReading(sensor_1,1547718199,14.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    10> SensorReading(sensor_4,1547718205,24.9)
    7> SensorReading(sensor_1,1547718199,14.8)
    10> SensorReading(sensor_4,1547718205,17.8)
    7> SensorReading(sensor_1,1547718199,11.8)
    3> SensorReading(sensor_2,1547718201,14.8)
    3> SensorReading(sensor_2,1547718201,14.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    10> SensorReading(sensor_4,1547718205,17.8)
    3> SensorReading(sensor_2,1547718201,14.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    7> SensorReading(sensor_1,1547718199,11.8)
     */

    // todo minBy 根据id进行分组  按sensor_id分组 返回目前位置 对应temperature最小的完整数据
    val aggStreamMinBy: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .minBy("temperature")

//    aggStreamMinBy.print()
    /*
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718201,15.4)
    10> SensorReading(sensor_4,1547718205,38.1)
    1> SensorReading(sensor_3,1547718202,6.7)
    7> SensorReading(sensor_1,1547718206,34.8)
    3> SensorReading(sensor_2,1547718201,15.4)
    7> SensorReading(sensor_1,1547718208,31.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    7> SensorReading(sensor_1,1547718211,14.8)
    10> SensorReading(sensor_4,1547718214,24.9)
    10> SensorReading(sensor_4,1547718218,17.8)
    7> SensorReading(sensor_1,1547718211,14.8)
    7> SensorReading(sensor_1,1547718222,11.8)
    3> SensorReading(sensor_2,1547718223,14.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    3> SensorReading(sensor_2,1547718223,14.8)
    10> SensorReading(sensor_4,1547718218,17.8)
    3> SensorReading(sensor_2,1547718223,14.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    7> SensorReading(sensor_1,1547718222,11.8)

     */

    // todo max 根据id进行分组  按sensor_id分组 返回目前位置 id对应的第一个出现的timeStamp 和最大的temperature
    val aggStreamMax: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .max("temperature")

//    aggStreamMax.print()
    /*
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718201,15.4)
    1> SensorReading(sensor_3,1547718202,6.7)
    7> SensorReading(sensor_1,1547718199,35.8)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718201,21.8)
    7> SensorReading(sensor_1,1547718199,35.8)
    10> SensorReading(sensor_4,1547718205,38.1)
    1> SensorReading(sensor_3,1547718202,24.7)
    7> SensorReading(sensor_1,1547718199,35.8)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718201,21.8)
    3> SensorReading(sensor_2,1547718201,21.8)
    1> SensorReading(sensor_3,1547718202,24.7)
    10> SensorReading(sensor_4,1547718205,38.1)
    3> SensorReading(sensor_2,1547718201,32.1)
    1> SensorReading(sensor_3,1547718202,33.2)
    7> SensorReading(sensor_1,1547718199,35.8)
     */

    // todo maxBy 根据id进行分组  按sensor_id分组 返回目前位置 id对应temperature最大的完整数据
    val aggStreamMaxBy: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .maxBy("temperature")

//    aggStreamMaxBy.print()
    /*
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718201,15.4)
    1> SensorReading(sensor_3,1547718202,6.7)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,35.8)
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718209,21.8)
    7> SensorReading(sensor_1,1547718199,35.8)
    1> SensorReading(sensor_3,1547718212,24.7)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,35.8)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718199,35.8)
    3> SensorReading(sensor_2,1547718209,21.8)
    3> SensorReading(sensor_2,1547718209,21.8)
    1> SensorReading(sensor_3,1547718212,24.7)
    10> SensorReading(sensor_4,1547718205,38.1)
    1> SensorReading(sensor_3,1547718233,33.2)
    3> SensorReading(sensor_2,1547718232,32.1)
    7> SensorReading(sensor_1,1547718199,35.8)
     */

    // todo sum 根据id进行分组  按sensor_id分组 返回目前位置 id对应的第一个出现的timeStamp 和temperature的累积和
    val aggStreamSum: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .sum("temperature")

//    aggStreamSum.print()

    env.execute("KeyBy")

  }
}
