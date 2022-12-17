package com.atguigu.flink
/*
DataStream -> Table 时定义rowtime 事件时间
 */

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object FlinkSql011_fromDataStream_rowtime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 默认就是ProcessingTime

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

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
      .assignTimestampsAndWatermarks(  // todo 有界的乱序程度可以确定的 过一段时间生成一个waterMark不是每条数据之后都带waterMark 适用于短时间内的密集数据
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {  // todo Time.seconds(3)是参数 最大乱序程度  waterMark在设置时就是当前最大EventTimeStamp - 3s  这个参数 给大了性能低  给小了结果不正确
          override def extractTimestamp(element: SensorReading): Long = {
            element.timestamp * 1000L // todo 时间戳
          }
        }
      )
    // todo 关键步骤 定义proctime
//    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime, 'temperature) // 这样原本额timestamp就被覆盖了
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'rt.rowtime)

    sensorTable.printSchema()
    /*
    root
     |-- id: STRING
     |-- timestamp: BIGINT
     |-- temperature: DOUBLE
     |-- rt: TIMESTAMP(3) *ROWTIME*
     */

    val resultDataStream: DataStream[Row] = sensorTable.toAppendStream[Row]

    resultDataStream.print()
    /*
    sensor_1,1547718199,35.8,2019-01-17 09:43:19.0
    sensor_2,1547718201,15.4,2019-01-17 09:43:21.0
    sensor_3,1547718202,6.7,2019-01-17 09:43:22.0
    sensor_4,1547718203,38.1,2019-01-17 09:43:23.0
    sensor_1,1547718206,34.8,2019-01-17 09:43:26.0
    sensor_1,1547718208,31.8,2019-01-17 09:43:28.0
    sensor_2,1547718209,21.8,2019-01-17 09:43:29.0
    sensor_1,1547718211,14.8,2019-01-17 09:43:31.0
    sensor_3,1547718212,24.7,2019-01-17 09:43:32.0
    sensor_4,1547718214,24.9,2019-01-17 09:43:34.0
    sensor_1,1547718215,15.8,2019-01-17 09:43:35.0
    sensor_1,1547718198,37.1,2019-01-17 09:43:18.0
    sensor_1,1547718197,7.1,2019-01-17 09:43:17.0
    sensor_4,1547718218,17.8,2019-01-17 09:43:38.0
    sensor_1,1547718196,5.1,2019-01-17 09:43:16.0
    sensor_1,1547718222,11.8,2019-01-17 09:43:42.0
    sensor_2,1547718223,14.8,2019-01-17 09:43:43.0
    sensor_2,1547718224,14.9,2019-01-17 09:43:44.0
    sensor_3,1547718226,15.7,2019-01-17 09:43:46.0
    sensor_4,1547718231,19.8,2019-01-17 09:43:51.0
    sensor_2,1547718232,32.1,2019-01-17 09:43:52.0
    sensor_3,1547718233,33.2,2019-01-17 09:43:53.0
    sensor_1,1547718234,13.4,2019-01-17 09:43:54.0
    sensor_1,1547718193,36.8,2019-01-17 09:43:13.0
    sensor_1,1547718193,5.8,2019-01-17 09:43:13.0
     */

    env.execute()
  }
}
