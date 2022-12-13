package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._ // 隐士转换  用flink-sql就要import这个


object FlinkSql001_example {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // todo DataStrem -> table, table -> view, DataStream -> view


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
//    dataStream.print()

    // todo 创建表执行环境  默认调用老版本planner 流式处理模式  也可以处理批
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // todo 基于有界流流创建表  dataStream -> Table
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // todo 1 调用table api进行转换
    val resultTable: Table = dataTable.select("id, temperature").filter('id === "sensor_1")
    resultTable.printSchema()
    /*
    root
     |-- id: STRING
     |-- temperature: DOUBLE
     */
//    resultTable.toAppendStream[(String, Double)].print("result")
    /*
    result> (sensor_1,35.8)
    result> (sensor_1,31.8)
    result> (sensor_1,34.8)
    result> (sensor_1,14.8)
    result> (sensor_1,15.8)
    result> (sensor_1,11.8)
    result> (sensor_1,13.4)
     */




    // todo 2 sql实现 TABLE -> VIEW
    // 注册表
    val tableName: String = "dataTableSql"
    tableEnv.createTemporaryView(tableName, dataTable)
    val sqlText =
      s"""
        |select
        |    id,
        |    temperature
        |from ${tableName}
        |where id = 'sensor_1'
        |""".stripMargin
    val resultSqlTable = tableEnv.sqlQuery(sqlText)
//    resultSqlTable.toAppendStream[(String, Double)].print("resultSql")

    /*
    resultSql> (sensor_1,35.8)
    resultSql> (sensor_1,34.8)
    resultSql> (sensor_1,31.8)
    resultSql> (sensor_1,14.8)
    resultSql> (sensor_1,15.8)
    resultSql> (sensor_1,37.1)
    resultSql> (sensor_1,7.1)
    resultSql> (sensor_1,5.1)
    resultSql> (sensor_1,11.8)
    resultSql> (sensor_1,13.4)
    resultSql> (sensor_1,36.8)
    resultSql> (sensor_1,5.8)
     */

    // todo 2 DataStream -> view
    tableEnv.createTemporaryView("dataTableSql2", dataStream)
    val sqlText1 =
      s"""
         |select
         |    id,
         |    temperature
         |from ${tableName}
         |where id = 'sensor_1'
         |""".stripMargin
    val resultSqlTable1 = tableEnv.sqlQuery(sqlText1)
      resultSqlTable1.toAppendStream[(String, Double)].print("resultSql1")
    /*
    resultSql1> (sensor_1,35.8)
    resultSql1> (sensor_1,34.8)
    resultSql1> (sensor_1,31.8)
    resultSql1> (sensor_1,14.8)
    resultSql1> (sensor_1,15.8)
    resultSql1> (sensor_1,37.1)
    resultSql1> (sensor_1,7.1)
    resultSql1> (sensor_1,5.1)
    resultSql1> (sensor_1,11.8)
     */


    env.execute()


  }
}
