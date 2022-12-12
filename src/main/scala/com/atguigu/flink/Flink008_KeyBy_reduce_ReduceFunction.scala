package com.atguigu.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Flink007_KeyBy_reduce {
  def main(args: Array[String]): Unit = {
    // todo KeyBy  DataStream -> KeyedStream 逻辑的将一个流拆分成不相交的分区，相同key的元素一定在一个分区中 一个分区 可能有多个key的元素
    // todo keyBy中传字段名或者idx都可以
    // todo 复杂的聚合  reduce
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

    // todo 想要根据sensor_id分组  temperature要目前为止的最小值  timeStamp要当前这条数据的
    // 这个需求用keyBy + min, keyBy + minBy都不行
    val resultStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce((curState, newData) => { // todo lambda表达式
        SensorReading(
          curState.id,
          newData.timestamp, // todo 最新这条数据的TimeStamp
          curState.temperature.min(newData.temperature) // todo 状态的temperature和最新的这条数据的较小值
        )
      })
//      .reduce(new MyReduceFunction) // todo 创建类 继承ReduceFunction[T]  不用lambda表达式就用这个



    resultStream.print()
    /*
    7> SensorReading(sensor_1,1547718199,35.8)
    1> SensorReading(sensor_3,1547718202,6.7)
    3> SensorReading(sensor_2,1547718201,15.4)
    10> SensorReading(sensor_4,1547718205,38.1)
    7> SensorReading(sensor_1,1547718206,34.8)
    7> SensorReading(sensor_1,1547718208,31.8)
    3> SensorReading(sensor_2,1547718209,15.4)
    1> SensorReading(sensor_3,1547718212,6.7)
    7> SensorReading(sensor_1,1547718211,14.8)
    10> SensorReading(sensor_4,1547718214,24.9)
    7> SensorReading(sensor_1,1547718216,14.8)
    10> SensorReading(sensor_4,1547718218,17.8)
    7> SensorReading(sensor_1,1547718222,11.8)
    3> SensorReading(sensor_2,1547718223,14.8)
    1> SensorReading(sensor_3,1547718226,6.7)
    3> SensorReading(sensor_2,1547718224,14.8)
    10> SensorReading(sensor_4,1547718231,17.8)
    3> SensorReading(sensor_2,1547718232,14.8)
    1> SensorReading(sensor_3,1547718233,6.7)
    7> SensorReading(sensor_1,1547718234,11.8)
     */

    env.execute("reduce")

  }
}

// TODO ReduceFunction  导入flick的  spark也有ReduceFunction
class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
  }
}