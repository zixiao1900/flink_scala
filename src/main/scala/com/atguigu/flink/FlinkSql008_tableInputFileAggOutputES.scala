package com.atguigu.flink
/*
flink sql 聚合操作
从file 读  然后flink sql聚合 写入ES
 */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}

object FlinkSql008_tableInputFileAggOutputES {
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

    // todo 简单转换 append输出
    val simpleTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // todo 聚合转换  无法输出到kafka
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id 分组
      .select('id, 'id.count as 'count)


    // todo Agg输出到ES
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("localhost", 9200, "http")
      .index("sensor_agg")
      .documentType("temperature")
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

// es部分
// cmd1: elasticsearch.bat
// 开打 http://localhost:9200/ 看到如下json就是启动成功
// 运行本代码 等待从kafka消费数据了
// 运行Flink005 生产数据到topic_flink_senor
// 可以查看cmd3就是从生产者topic消费到的数据
// 查看有哪些index cmd2: curl "localhost:9200/_cat/indices?v"
// 查看flink sql处理从kafka消费的数据写入es之后的样子 cmd2: curl "localhost:9200/sensor_agg/_search?pretty"

/* todo 根据sensor_id 聚合后的count结果
{
  "took" : 1,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 4,
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "sensor_agg",
        "_type" : "temperature",
        "_id" : "sensor_4",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_4",
          "cnt" : 4
        }
      },
      {
        "_index" : "sensor_agg",
        "_type" : "temperature",
        "_id" : "sensor_3",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_3",
          "cnt" : 4
        }
      },
      {
        "_index" : "sensor_agg",
        "_type" : "temperature",
        "_id" : "sensor_1",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_1",
          "cnt" : 12
        }
      },
      {
        "_index" : "sensor_agg",
        "_type" : "temperature",
        "_id" : "sensor_2",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_2",
          "cnt" : 5
        }
      }
    ]
  }
}
 */
