package com.atguigu.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.scala._

object FlinkSql002_tableEnv {
  def main(args: Array[String]): Unit = {
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 批文件
    val inputBatchDataSet: DataSet[String] = batchEnv.readTextFile("datas/sensor2.txt")

    // 文件模拟数据流  一次性读取 有界流 可以写入
    val inputBatchStream: DataStream[String] = env.readTextFile("datas/sensor2.txt") // todo batch一次性读取
    // 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile1Line("datas/sensor2.txt"))

    // 有界流
    val dataBatchStream: DataStream[SensorReading] = inputBatchStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // 无界流
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )



    // todo 默认调用老版本planner + StreamMode  可处理有界流  无界流
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // todo 1.1 基于老版本的流处理  可处理有界流  无界流
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val oldStreamTable: Table = oldStreamTableEnv.fromDataStream(dataStream)
      .select("id, temperature, timestamp")
      .filter('id === "sensor_1")

//    oldStreamTable.toAppendStream[(String, Double, Long)].print("oldStreamTable")
    /*
    oldStreamTable> (sensor_1,35.8,1547718199)
    oldStreamTable> (sensor_1,34.8,1547718206)
    oldStreamTable> (sensor_1,31.8,1547718208)
    oldStreamTable> (sensor_1,14.8,1547718211)
     */

    // todo 1.2 基于老版本做批处理

    val oldBatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    // todo 2.1 基于blink planner的流处理  可处理有界流  无界流
    val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings)
    val blinkStreamTable: Table = blinkStreamTableEnv.fromDataStream(dataStream)
      .select("id, temperature, timestamp")
      .filter('id === "sensor_1")
//    blinkStreamTable.toAppendStream[(String, Double, Long)].print("blinkStreamTable")
    /*
    oldStreamTable> (sensor_1,35.8,1547718199)
    oldStreamTable> (sensor_1,34.8,1547718206)
    oldStreamTable> (sensor_1,31.8,1547718208)
    oldStreamTable> (sensor_1,14.8,1547718211)
     */

    // todo 2.2 基于blink planner的批处理
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)


    env.execute()

  }
}
