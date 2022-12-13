package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object FlinkSql007_tableSimpleOuputKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // kafka Pipline
    // todo 从kafka读取成sql table 一次性读取文件 类似批操作
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("topic_flink_sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    ).withFormat( new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    // todo 转换操作
    val sensorTable: Table = tableEnv.from("kafkaInputTable")

    // todo 简单转换 append输出
    val simpleTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // todo 聚合转换  无法输出到kafka
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id 分组
      .select('id, 'id.count as 'count)


    // todo simple输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("topic_flink_sensor2")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    ).withFormat( new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    simpleTable.insertInto("kafkaOutputTable")

    env.execute()

  }
}

// 1启动kafka
// 2 开启kafka-consumer topic_flink_sensor2 观察最终写入kafka的数据
// 3运行FlinkSql007等待从topic_flink_sensor读取数据为table并转换 然后写入kafka topic_flink_sensor2
// 4 运行Flink005 生产数据到topic_flink_senor 观察第二步
