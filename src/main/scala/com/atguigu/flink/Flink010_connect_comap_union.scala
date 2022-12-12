package com.atguigu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

object Flink010_connect_comap_union {
  def main(args: Array[String]): Unit = {
    // todo split   DataStream -> SplitStream: 根据某些特征把一个DataStream拆分成两个或者多个DataStream
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt")) // todo 流式1行或多行读取

    // 转为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // todo 分流操作 split + select
    val splitStream: SplitStream[SensorReading] = dataStream
      .split(data => {
        if (data.temperature > 30.0) {
          List("high")
        }
        else {
          List("low")
        }
      })

    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val warningStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))

    // todo 合流操作connect 两个来源数据类型不用相同 DataStream, DataStream -> ConnectedStream  只能connect两个
    // 连接两个保持他们类型的数据流，两个数据流被connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式
    // warningStream和lowStream可以是不同类型的
    val connectStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowStream)

    // todo 两个流互相独立  用coMap/coFlatMap 作用于ConnectedStream上  connectedStream -> DataStream
    val coMapStream: DataStream[Product] = connectStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
//    coMapStream.print("coMap")
    /*  不同流 先用connect合并(DataStream, DataStream -> ConnectedStreams) 再用coMap转换后 ConnectedStreams -> DataStream
    coMap:11> (sensor_2,healthy)
    coMap:10> (sensor_1,35.8,warning)
    coMap:12> (sensor_3,healthy)
    coMap:1> (sensor_4,38.1,warning)
    coMap:2> (sensor_1,34.8,warning)
    coMap:3> (sensor_1,31.8,warning)
    coMap:4> (sensor_2,healthy)
    coMap:5> (sensor_1,healthy)
    coMap:6> (sensor_3,healthy)
    coMap:7> (sensor_4,healthy)
    coMap:8> (sensor_1,healthy)
    coMap:10> (sensor_1,healthy)
    coMap:9> (sensor_4,healthy)
    coMap:12> (sensor_2,healthy)
    coMap:11> (sensor_2,healthy)
    coMap:1> (sensor_3,healthy)
    coMap:2> (sensor_4,healthy)
    coMap:3> (sensor_2,32.1,warning)
    coMap:4> (sensor_3,33.2,warning)
    coMap:5> (sensor_1,healthy)
     */

    // todo union 合并多个数据类型相同的DataStream (DataStream, DataStream -> DataStream)  可以union多个
    val unionStream: DataStream[SensorReading] = highStream.union(lowStream)
    unionStream.print("union")
    /*
    union:9> SensorReading(sensor_1,1547718199,35.8)
    union:11> SensorReading(sensor_3,1547718202,6.7)
    union:10> SensorReading(sensor_2,1547718201,15.4)
    union:12> SensorReading(sensor_4,1547718205,38.1)
    union:1> SensorReading(sensor_1,1547718206,34.8)
    union:2> SensorReading(sensor_1,1547718208,31.8)
    union:3> SensorReading(sensor_2,1547718209,21.8)
    union:4> SensorReading(sensor_1,1547718211,14.8)
    union:5> SensorReading(sensor_3,1547718212,24.7)
    union:6> SensorReading(sensor_4,1547718214,24.9)
    union:7> SensorReading(sensor_1,1547718216,15.8)
    union:8> SensorReading(sensor_4,1547718218,17.8)
    union:9> SensorReading(sensor_1,1547718222,11.8)
    union:10> SensorReading(sensor_2,1547718223,14.8)
    union:11> SensorReading(sensor_2,1547718224,14.9)
    union:12> SensorReading(sensor_3,1547718226,15.7)
    union:2> SensorReading(sensor_2,1547718232,32.1)
    union:1> SensorReading(sensor_4,1547718231,19.8)
    union:3> SensorReading(sensor_3,1547718233,33.2)
    union:4> SensorReading(sensor_1,1547718234,13.4)

     */

    env.execute("connect_coMap")

  }
}
