package com.atguigu.flink

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.util.Random

object Flink005_kafka_MockData {

    def main(args: Array[String]): Unit = {
        // todo 由文件向kafka流试生产数据 每两秒读取一行或者多行数据   自己定义的！！！
        val kafkaGenerator = new MykafkaGenerateDataFromFile(
            "localhost:9092",
            "datas/sensor1.txt",
            "topic_flink_sensor"
        )
        kafkaGenerator.run()
    }

}

// todo 把从文件冲一次读取一行或几行 生产进kafka抽象成类
class MykafkaGenerateDataFromFile(host: String, dataPath:String, produceTopic:String) {

    // todo kafka添加配置
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host) // 集群地址
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // todo kafka生产者
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    // todo 读取文件 拿到Iterator[String]
    val source: BufferedSource = Source.fromFile(dataPath)
    val lines: Iterator[String] = source.getLines()

    // 每次从文件中读取一行或两行数据
    def generateData(lines: Iterator[String]) = {
        val list: ListBuffer[String] = ListBuffer[String]()
        var num = new Random().nextInt(2) + 1 //  1 - 2条
        while (lines.hasNext && num > 0) {
            list.append(lines.next())
            num -= 1
        }
        list
    }

    def run(): Unit = {
        while ( true ) {
            // todo 每次按文件顺序获取几条数据
            val tmpList: ListBuffer[String] = generateData(lines)
            tmpList.foreach(
                data => {
                    // todo 向Kafka中生成数据
                    val record = new ProducerRecord[String, String](produceTopic, data)
                    // 生产者发送数据
                    producer.send(record)
                    println(data)
                }
            )
            // 每两秒发一次  一次随机生成1-2条数据(String)
            Thread.sleep(2000)
        }
    }
}
// todo 使用方法
// 启动zookeeper cmd1: bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
// 启动kafka  cmd2: bin/windows/kafka-server-start.bat ./config/server.properties
// 启动程序 这个是kafka生产者 生产数据的
// 然后启动程序 程序是kafka生产者  生产的数据会在消费者中监听到
