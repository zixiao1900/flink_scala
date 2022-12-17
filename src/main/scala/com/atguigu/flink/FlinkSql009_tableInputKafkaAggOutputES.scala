package com.atguigu.flink

/*
flink sql 聚合操作
从kafka 读  然后flink sql聚合 写入ES
 */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors._

object FlinkSql009_tableInputKafkaAggOutputES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // todo 从kafka读取成sql table 一次性读取文件 类似批操作
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


    // todo 动态表
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    // todo 简单转换 append输出
    val simpleTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // todo 聚合转换 带有状态 无法输出到kafka
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id 分组
      .select('id, 'id.count as 'count)


    // todo Agg输出到ES
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("localhost", 9200, "http")
      .index("sensor_stream_agg")
      .documentType("temperature")
      .bulkFlushMaxActions(1) // todo 流式的写入 这里千万不能少 不然写不进去的
    ).inUpsertMode()  // todo 聚合更新模式
      .withFormat(new Json()) // pom中加入依赖flink-json
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")

    aggTable.insertInto("esOutputTable")

    env.execute("es output test")

  }
}
// kafka部分
//-- 启动zookeeper 不要关
//  bash1: bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
//  -- 启动kafa  不要关
//  bash2: bin/windows/kafka-server-start.bat ./config/server.properties
//  监听一下kafka传入的数据
//  bash3: bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic topic_flink_sensor

// es部分
// cmd1: elasticsearch.bat
// 开打 http://localhost:9200/ 看到如下json就是启动成功
// 运行本代码 等待从kafka消费数据了
// 运行Flink005 生产数据到topic_flink_senor
// 可以查看cmd3就是从生产者topic消费到的数据
// 查看有哪些index cmd2: curl "localhost:9200/_cat/indices?v"
// 查看flink sql处理从kafka消费的数据写入es之后的样子 cmd2: curl "localhost:9200/sensor_agg/_search?pretty"

