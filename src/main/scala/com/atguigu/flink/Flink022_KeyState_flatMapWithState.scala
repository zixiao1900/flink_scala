package com.atguigu.flink

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Map
import java.{lang, util}

// todo KeyState 状态编程

object Flink022_KeyState_flatMapWithState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 文件模拟数据流  一次性读取 有界流 可以写入
    //    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile1Line("datas/sensor2.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
    dataStream.print()

    // todo 需求 对于温度传感器 温度跳变超过10度报警
    val alertSteam = dataStream
      .keyBy(data => data.id)
      .flatMapWithState[(String, Double, Double), Double]({ // todo flatMapWithState必须在keyedStream才能用 是监键控状态
        // todo 初始状态
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        // todo 非初始状态
        case (data: SensorReading, lastTemp: Some[Double]) => {
          // 跟最新的温度值做差值
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            // todo 报警的返回值
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          }
          else {
            // todo 不报警也一定要有返回值
            (List.empty, Some(data.temperature))
          }
        }
      })

    alertSteam.print()
    /* todo 根据sensor_id分组 组内温度变化超过10度就报警  效果和上一个代码一样
    SensorReading(sensor_1,1547718199,35.8)
    SensorReading(sensor_2,1547718201,15.4)
    SensorReading(sensor_3,1547718202,6.7)
    SensorReading(sensor_4,1547718203,38.1)
    SensorReading(sensor_1,1547718206,34.8)
    SensorReading(sensor_1,1547718208,31.8)
    SensorReading(sensor_2,1547718209,21.8)
    SensorReading(sensor_1,1547718211,14.8)
    (sensor_1,31.8,14.8)
    SensorReading(sensor_3,1547718212,24.7)
    (sensor_3,6.7,24.7)
    SensorReading(sensor_4,1547718214,24.9)
    (sensor_4,38.1,24.9)
    SensorReading(sensor_1,1547718215,15.8)
    SensorReading(sensor_1,1547718198,37.1)
    (sensor_1,15.8,37.1)
    SensorReading(sensor_1,1547718197,7.1)
    (sensor_1,37.1,7.1)
    SensorReading(sensor_4,1547718218,17.8)
    SensorReading(sensor_1,1547718196,5.1)
    SensorReading(sensor_1,1547718222,11.8)
    SensorReading(sensor_2,1547718223,14.8)
    SensorReading(sensor_2,1547718224,14.9)
    SensorReading(sensor_3,1547718226,15.7)
    SensorReading(sensor_4,1547718231,19.8)
    SensorReading(sensor_2,1547718232,32.1)
    (sensor_2,14.9,32.1)
    SensorReading(sensor_3,1547718233,33.2)
    (sensor_3,15.7,33.2)
    SensorReading(sensor_1,1547718234,13.4)
    SensorReading(sensor_1,1547718193,36.8)
    (sensor_1,13.4,36.8)
    SensorReading(sensor_1,1547718193,5.8)
    (sensor_1,36.8,5.8)
     */


    env.execute("flatMapWithState")
  }
}



