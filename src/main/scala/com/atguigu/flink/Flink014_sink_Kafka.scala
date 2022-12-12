package com.atguigu.flink

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties


object Flink014_sink_Kafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


    // todo 从kafka 消费数据
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
    /*
    sensor_1,1547718199,35.8
    sensor_2,1547718201,15.4
    sensor_3,1547718202,6.7
    sensor_4,1547718205,38.1
    sensor_1,1547718206,34.8
     */

    // todo etl
    val dataStream: DataStream[String] = kafkaDataStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      }
    )

    // todo 写入kafka 另一个topic kafka生产者
    dataStream.addSink(
      new FlinkKafkaProducer011[String](
        "localhost:9092",
        "kafka_sink_test01",
        new SimpleStringSchema()

      )
    )
    /*
    SensorReading(sensor_1,1547718199,35.8)
    SensorReading(sensor_2,1547718201,15.4)
    SensorReading(sensor_3,1547718202,6.7)
    SensorReading(sensor_4,1547718205,38.1)
    SensorReading(sensor_1,1547718206,34.8)
     */


    env.execute("sink_Kafka")
  }
}

// todo
//  1 运行Flink014_sink_kafka 从topic_flink_sensor2中消费数据 做etl 再写入kafka_sink_test01中
//  2 way1: 运行Flink005_kafka_MockData往topic_flink_sensor2中生产数据据
//    way2: bin/windows/kafka-console-producer.bat --broker-list localhost:9092 --topic topic_flink_sensor2 直接向topic_flink_sensor2手动写
//  3 关注  topic_flink_sensor2是生产数据的   kafka_sink_test01是读取topic中的数据做etl再写入另一个topic的

