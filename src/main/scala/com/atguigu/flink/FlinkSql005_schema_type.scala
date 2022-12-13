package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _} // 隐士转换  用flink-sql就要import这个


object FlinkSql005_schema_type {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 文件模拟数据流  一次性读取 有界流 可以写入
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor2.txt") // todo batch一次性读取
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

    // todo 创建表执行环境  默认调用老版本planner 流式处理模式
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // todo 基于有界流流创建表  dataStream -> Table  变换顺序 重命名
    val dataTable: Table = tableEnv
      .fromDataStream(dataStream, 'my_id, 'ts, 'temp)

    // todo 1 调用table api进行转换
    val resultTable: Table = dataTable.select("temp, my_id, ts").filter('my_id === "sensor_1")
    resultTable.printSchema()
    /*
   root
     |-- temp: DOUBLE
     |-- my_id: STRING
     |-- ts: BIGINT
     */
    resultTable.toAppendStream[(Double, String, Long)].print("result")
    /*
    result> (35.8,sensor_1,1547718199)
    result> (34.8,sensor_1,1547718206)
    result> (31.8,sensor_1,1547718208)
    result> (14.8,sensor_1,1547718211)
    result> (15.8,sensor_1,1547718215)
    result> (37.1,sensor_1,1547718198)
    result> (7.1,sensor_1,1547718197)
    result> (5.1,sensor_1,1547718196)
    result> (11.8,sensor_1,1547718222)
    result> (13.4,sensor_1,1547718234)
    result> (36.8,sensor_1,1547718193)
    result> (5.8,sensor_1,1547718193)
     */




    env.execute()


  }
}
