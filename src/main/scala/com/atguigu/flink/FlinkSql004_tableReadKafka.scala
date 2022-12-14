package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object FlinkSql004_tableReadKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 输入表
    // todo 从kafka读取成sql table
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("topic_flink_sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    ).withFormat( new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    // todo sql 每一条都做个过滤
    val sqlText =
      s"""
         |select * from kafkaInputTable where id = 'sensor_1'
         |""".stripMargin
    val resultTableSql = tableEnv.sqlQuery(sqlText)
//    resultTableSql.toAppendStream[(String, Long, Double)].print()
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
    val resultTable = tableEnv.from("kafkaInputTable").select("*").filter('id === "sensor_1")
    val resultDataStream: DataStream[(String, Long, Double)] = resultTable.toAppendStream[(String, Long, Double)]
    resultDataStream.print()
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
