package com.atguigu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Flink015_sink_Redis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 文件模拟数据流
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // todo 定义一个flinkJedisConfigBase
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()



    dataStream.addSink(
      new RedisSink[SensorReading](
        conf, // redis config
        new MyRedisMapper // redisMapper
      )
    )

    env.execute("sink_Redis")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
  // 定义保存数据写入redis的命令  Hset   key  field  value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp") // 获取表名
  }

  // 将温度值指定为value
  override def getKeyFromData(data: SensorReading): String = {
    data.id
  }

  // 将id指定为key
  override def getValueFromData(data: SensorReading): String = {
    data.temperature.toString
  }
}


// 使用 cmd1: redis-server 不要关
// cmd2: redis-cli
// 查看所有key cmd2: keys *
// 运行Flink015_sink_Redis
// 查询表中的sensor_1对应的value: hget sensor_temp sensor_1  如果由多个sensor_1 返回最新的那个
// 查询表中所有的k-v: hgetall sensor_temp