package com.atguigu.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

object Flink004_source_fromKafka {
  def main(args: Array[String]): Unit = {
    // 流任务环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    // kafka param
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // todo addSource输入函数  返回DataStream  连接KAFKA 消费kafka的数据
    val kafkaDataStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String](
        "topic_flink_sensor2",
        new SimpleStringSchema(),
        properties
      )
    )

    kafkaDataStream.print()
    /*

    kafka生产者输入
    sensor_1,1547718199,35.8
    sensor_6,1547718201,15.4
    sensor_7,1547718202,6.7
    sensor_10,1547718205,38.1

    控制台打印
    8> sensor_1,1547718199,35.8
    8> sensor_6,1547718201,15.4
    8> sensor_7,1547718202,6.7
    8> sensor_10,1547718205,38.1
     */

    // 先执行Flink005_kafka_MockData 从文件生产数据到kafka  再执行本代码从kafka生产的数据中进行消费数据
    /*
    1> sensor_1,1547718199,35.8
    1> sensor_2,1547718201,15.4
    12> sensor_3,1547718202,6.7
    1> sensor_4,1547718205,38.1
    1> sensor_1,1547718206,34.8
    12> sensor_1,1547718208,31.8
    12> sensor_2,1547718209,21.8
    1> sensor_1,1547718211,14.8
    1> sensor_3,1547718212,24.7
    12> sensor_4,1547718214,24.9
    12> sensor_1,1547718216,15.8
    1> sensor_4,1547718218,17.8
    12> sensor_1,1547718222,11.8
    1> sensor_2,1547718223,14.8
    1> sensor_2,1547718224,14.9
    12> sensor_3,1547718226,15.7
    1> sensor_4,1547718231,19.8
    1> sensor_2,1547718232,32.1
    12> sensor_3,1547718233,33.2
    12> sensor_1,1547718234,13.4
     */



    // todo 流处理执行
    env.execute("sourceFromKafka")
  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}


// Flink005_kafka_MockData 生产数据到kafka
// Flink004_source_fromKafka 从kafka生产者中消费数据



