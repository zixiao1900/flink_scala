package com.atguigu.flink
/*
DataStream -> Table 时定义proctime
 */

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object FlinkSql010_fromDataStream_proctime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) // 默认就是ProcessingTime

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
    // todo 关键步骤 定义proctime
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)

    sensorTable.printSchema()
    /*
    root
     |-- id: STRING
     |-- temperature: DOUBLE
     |-- timestamp: BIGINT
     |-- pt: TIMESTAMP(3) *PROCTIME*
     */

    val resultDataStream: DataStream[Row] = sensorTable.toAppendStream[Row]

    resultDataStream.print()
    /*
    sensor_1,35.8,1547718199,2022-12-17T13:15:44.494
    sensor_2,15.4,1547718201,2022-12-17T13:15:46.501
    sensor_3,6.7,1547718202,2022-12-17T13:15:48.503
    sensor_4,38.1,1547718203,2022-12-17T13:15:50.518
    sensor_1,34.8,1547718206,2022-12-17T13:15:52.520
    sensor_1,31.8,1547718208,2022-12-17T13:15:54.532
    sensor_2,21.8,1547718209,2022-12-17T13:15:56.547
    sensor_1,14.8,1547718211,2022-12-17T13:15:58.550
    sensor_3,24.7,1547718212,2022-12-17T13:16:00.564
    sensor_4,24.9,1547718214,2022-12-17T13:16:02.565
    sensor_1,15.8,1547718215,2022-12-17T13:16:04.565
    sensor_1,37.1,1547718198,2022-12-17T13:16:06.572
    sensor_1,7.1,1547718197,2022-12-17T13:16:08.588
    sensor_4,17.8,1547718218,2022-12-17T13:16:10.600
    sensor_1,5.1,1547718196,2022-12-17T13:16:12.602
    sensor_1,11.8,1547718222,2022-12-17T13:16:14.618
    sensor_2,14.8,1547718223,2022-12-17T13:16:16.631
    sensor_2,14.9,1547718224,2022-12-17T13:16:18.647
    sensor_3,15.7,1547718226,2022-12-17T13:16:20.648
    sensor_4,19.8,1547718231,2022-12-17T13:16:22.649
    sensor_2,32.1,1547718232,2022-12-17T13:16:24.661
    sensor_3,33.2,1547718233,2022-12-17T13:16:26.672
    sensor_1,13.4,1547718234,2022-12-17T13:16:28.686
    sensor_1,36.8,1547718193,2022-12-17T13:16:30.701
    sensor_1,5.8,1547718193,2022-12-17T13:16:32.704
     */

    env.execute()
  }
}
