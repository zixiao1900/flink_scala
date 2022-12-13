package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FlinkSql006_tableSimpleOutputFile {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // todo 从文件读取成sql table 一次性读取文件 类似批操作
    val filePath = "datas/sensor2.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) // 文件格式
      .withSchema(new Schema() // 定义schema
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // todo 转换操作
    val sensorTable: Table = tableEnv.from("inputTable")

    // todo 简单转换
    val simpleTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // todo 聚合转换  toRetractStream  聚合模式不能用append模式(insertInto)输出到文件 也不能输出到kafka 暂时print看一下结果
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id 分组
      .select('id, 'id.count as 'count)
    aggTable.toRetractStream[(String, Long)].print()
    /*  todo false代表作废
    (true,(sensor_1,1))
    (true,(sensor_2,1))
    (true,(sensor_3,1))
    (true,(sensor_4,1))
    (false,(sensor_1,1))
    (true,(sensor_1,2))
    (false,(sensor_1,2))
    (true,(sensor_1,3))
    (false,(sensor_2,1))
    (true,(sensor_2,2))
    (false,(sensor_1,3))
    (true,(sensor_1,4))
    (false,(sensor_3,1))
    (true,(sensor_3,2))
    (false,(sensor_4,1))
    (true,(sensor_4,2))
    (false,(sensor_1,4))
    (true,(sensor_1,5))
    (false,(sensor_1,5))
    (true,(sensor_1,6))
    (false,(sensor_1,6))
    (true,(sensor_1,7))
    (false,(sensor_4,2))
    (true,(sensor_4,3))
    (false,(sensor_1,7))
    (true,(sensor_1,8))
    (false,(sensor_1,8))
    (true,(sensor_1,9))
    (false,(sensor_2,2))
    (true,(sensor_2,3))
    (false,(sensor_2,3))
    (true,(sensor_2,4))
    (false,(sensor_3,2))
    (true,(sensor_3,3))
    (false,(sensor_4,3))
    (true,(sensor_4,4))
    (false,(sensor_2,4))
    (true,(sensor_2,5))
    (false,(sensor_3,3))
    (true,(sensor_3,4))
    (false,(sensor_1,9))
    (true,(sensor_1,10))
    (false,(sensor_1,10))
    (true,(sensor_1,11))
    (false,(sensor_1,11))
    (true,(sensor_1,12))
     */

    // todo simple输出到文件 注册输出表  和读取时一摸一样
    val fileOutputPath = "datas/sensor2_output_simple.txt"
    tableEnv.connect(new FileSystem().path(fileOutputPath))
      .withFormat(new Csv()) // 文件格式
      .withSchema(new Schema() // 定义schema
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable_simple")
    // 追加 不能有聚合的操作
    simpleTable.insertInto("outputTable_simple")


    env.execute()

  }
}
