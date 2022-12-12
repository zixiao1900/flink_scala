package com.atguigu.flink

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// todo ProcessFunction最底层的操作  process()

object Flink024_ProcessFunction_sideOutput {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 文件模拟数据流  一次性读取 有界流 可以写入
    //    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile1Line("datas/sensor2.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
    dataStream.print()

    // todo 需求 分流操作  >=30就是主流  <30就放入侧输出流
    val highTempStream = dataStream
      .process(
        new SplitTempProcessor(30.0)
      )

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    /*  todo 分流输出
    SensorReading(sensor_1,1547718199,35.8)
    high> SensorReading(sensor_1,1547718199,35.8)
    SensorReading(sensor_2,1547718201,15.4)
    low> (sensor_2,1547718201,15.4)
    SensorReading(sensor_3,1547718202,6.7)
    low> (sensor_3,1547718202,6.7)
    SensorReading(sensor_4,1547718203,38.1)
    high> SensorReading(sensor_4,1547718203,38.1)
    SensorReading(sensor_1,1547718206,34.8)
    high> SensorReading(sensor_1,1547718206,34.8)
    SensorReading(sensor_1,1547718208,31.8)
    high> SensorReading(sensor_1,1547718208,31.8)
    SensorReading(sensor_2,1547718209,21.8)
    low> (sensor_2,1547718209,21.8)
    SensorReading(sensor_1,1547718211,14.8)
    low> (sensor_1,1547718211,14.8)
    SensorReading(sensor_3,1547718212,24.7)
    low> (sensor_3,1547718212,24.7)
    SensorReading(sensor_4,1547718214,24.9)
    low> (sensor_4,1547718214,24.9)
    SensorReading(sensor_1,1547718215,15.8)
    low> (sensor_1,1547718215,15.8)
    SensorReading(sensor_1,1547718198,37.1)
    high> SensorReading(sensor_1,1547718198,37.1)
    SensorReading(sensor_1,1547718197,7.1)
    low> (sensor_1,1547718197,7.1)
    SensorReading(sensor_4,1547718218,17.8)
    low> (sensor_4,1547718218,17.8)
    SensorReading(sensor_1,1547718196,5.1)
    low> (sensor_1,1547718196,5.1)
    SensorReading(sensor_1,1547718222,11.8)
    low> (sensor_1,1547718222,11.8)
    SensorReading(sensor_2,1547718223,14.8)
    low> (sensor_2,1547718223,14.8)
    SensorReading(sensor_2,1547718224,14.9)
    low> (sensor_2,1547718224,14.9)
    SensorReading(sensor_3,1547718226,15.7)
    low> (sensor_3,1547718226,15.7)
    SensorReading(sensor_4,1547718231,19.8)
    low> (sensor_4,1547718231,19.8)
    SensorReading(sensor_2,1547718232,32.1)
    high> SensorReading(sensor_2,1547718232,32.1)
    SensorReading(sensor_3,1547718233,33.2)
    high> SensorReading(sensor_3,1547718233,33.2)
    SensorReading(sensor_1,1547718234,13.4)
    low> (sensor_1,1547718234,13.4)
    SensorReading(sensor_1,1547718193,36.8)
    high> SensorReading(sensor_1,1547718193,36.8)
    SensorReading(sensor_1,1547718193,5.8)
    low> (sensor_1,1547718193,5.8)

     */





    env.execute("processFunction-sideOutput")
  }
}

// 不做keyBy 直接DataStream.process
// todo 实现利用侧输出流进行分流的processFunction                          输入类型        主流的输出类型
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature >= threshold) {
      // todo 如果当前文帝>=30 输出到主流
      out.collect(value)
    }
    else {
      // todo 小于30输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    }
  }
}




