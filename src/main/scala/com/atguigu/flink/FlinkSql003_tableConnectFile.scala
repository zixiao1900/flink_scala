package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FlinkSql003_tableConnectFile {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // todo 从文件读取成sql table 一次性读取文件 类似批操作
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val filePath = "datas/sensor2.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) // 文件格式
      .withSchema(new Schema() // 定义schema
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // todo sql 做过滤
    val sqlText =
      s"""
         |select * from inputTable where id = 'sensor_1'
         |""".stripMargin
    val resultTableSql = tableEnv.sqlQuery(sqlText)
    resultTableSql.toAppendStream[(String, Long, Double)].print()
    /*
    (sensor_1,1547718199,35.8)
    (sensor_1,1547718206,34.8)
    (sensor_1,1547718208,31.8)
    (sensor_1,1547718211,14.8)
    (sensor_1,1547718215,15.8)
    (sensor_1,1547718198,37.1)
    (sensor_1,1547718197,7.1)
    (sensor_1,1547718196,5.1)
    (sensor_1,1547718222,11.8)
    (sensor_1,1547718234,13.4)
    (sensor_1,1547718193,36.8)
    (sensor_1,1547718193,5.8)
     */

    // todo table API 做过滤
    val resultTable = tableEnv.from("inputTable").select("*").filter('id === "sensor_1")
//    resultTable.toAppendStream[(String, Long, Double)].print()
    /*
    (sensor_1,1547718199,35.8)
    (sensor_1,1547718206,34.8)
    (sensor_1,1547718208,31.8)
    (sensor_1,1547718211,14.8)
    (sensor_1,1547718215,15.8)
    (sensor_1,1547718198,37.1)
    (sensor_1,1547718197,7.1)
    (sensor_1,1547718196,5.1)
    (sensor_1,1547718222,11.8)
    (sensor_1,1547718234,13.4)
    (sensor_1,1547718193,36.8)
    (sensor_1,1547718193,5.8)

Process finished with exit code 0

     */
    env.execute()

  }
}
