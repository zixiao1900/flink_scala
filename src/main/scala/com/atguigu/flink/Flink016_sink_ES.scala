package com.atguigu.flink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util

object Flink016_sink_ES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 文件模拟数据流  一次性读取 有界流 可以写入
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // todo 写入无界流的DataStream 需要设置  esSinkBuilder.setBulkFlushMaxActions(1)
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // todo 定义httpHosts
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    // 增加节点 windows下就是单节点
    httpHosts.add(new HttpHost("localhost", 9200))

    // todo 自定义EsSinkFunc
    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      // 每来一个数据都会调用process方法
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 包装一个Map作为data source
        val dataDource: util.HashMap[String, String] = new util.HashMap[String, String]()
        dataDource.put("id", t.id)
        dataDource.put("temperature", t.temperature.toString)
        dataDource.put("ts", t.timestamp.toString)

        // 创建index request 用于发送 http请求
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensorstream") // todo 必须是小写
          .`type`("readingData")
          .source(dataDource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    }

    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      myEsSinkFunc
    )
    // todo 批量请求的配置；这将指示接收器在每个元素之后发出请求，否则将对它们进行缓冲。  如果写入有界流 就不需要这个
    esSinkBuilder.setBulkFlushMaxActions(1)

    dataStream.addSink(
      esSinkBuilder.build()
    )



    env.execute("sink_ES")
  }
}



// cmd1: elasticsearch.bat
// 开打 http://localhost:9200/ 看到如下json就是启动成功
// 运行本代码

// 也可以直接在浏览器中输入url看
// 查看有哪些index cmd2: curl "localhost:9200/_cat/indices?v"
//health status index  uuid                   pri rep docs.count docs.deleted store.size pri.store.size
//yellow open   sensor CiTJd6UBRtCV9zSp7hd4Eg   5   1         20            0       460b           460b  // 有界流

// 查看具体index中的数据 cmd2: curl "localhost:9200/sensor/_search?pretty"
/*
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
    "total" : 20,
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "4tYr5oQBzS1TVbFmG9DZ",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "38.1",
          "id" : "sensor_4",
          "ts" : "1547718205"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "5dYr5oQBzS1TVbFmM9BS",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "21.8",
          "id" : "sensor_2",
          "ts" : "1547718209"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "7tYr5oQBzS1TVbFmWtCJ",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "15.7",
          "id" : "sensor_3",
          "ts" : "1547718226"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "79Yr5oQBzS1TVbFmYtA1",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "19.8",
          "id" : "sensor_4",
          "ts" : "1547718231"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "8dYr5oQBzS1TVbFmatBI",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "32.1",
          "id" : "sensor_2",
          "ts" : "1547718232"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "4NYr5oQBzS1TVbFmDNAP",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "15.4",
          "id" : "sensor_2",
          "ts" : "1547718201"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "4dYr5oQBzS1TVbFmE9DG",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "6.7",
          "id" : "sensor_3",
          "ts" : "1547718202"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "59Yr5oQBzS1TVbFmOtDz",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "14.8",
          "id" : "sensor_1",
          "ts" : "1547718211"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "8tYr5oQBzS1TVbFmcdD2",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "13.4",
          "id" : "sensor_1",
          "ts" : "1547718234"
        }
      },
      {
        "_index" : "sensorstream",
        "_type" : "readingData",
        "_id" : "6NYr5oQBzS1TVbFmQ9AP",
        "_score" : 1.0,
        "_source" : {
          "temperature" : "24.9",
          "id" : "sensor_4",
          "ts" : "1547718214"
        }
      }
    ]
  }
}
 */