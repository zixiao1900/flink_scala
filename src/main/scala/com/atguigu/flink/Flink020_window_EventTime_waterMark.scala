package com.atguigu.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink020_window_EventTime_waterMark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // todo 1.设置使用EventTime
    // todo 2.dataStream.assignTimestampsAndWatermarks 设置waterMark指定数据中的timeStamp
    // todo 3.keyBy + window
    // todo 4.allowedLateness允许迟到数据
    // todo 5.sideOutputLateData侧输出流兜底
    // todo 6.reduce 聚合
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // todo 默认是ProcessingTime
    env.getConfig.setAutoWatermarkInterval(500) // todo 争对密集型数据 使用周期性的waterMark 默认周期时间是200ms

    // 文件模拟数据流  一次性读取 有界流 可以写入
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // todo 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile1Line("datas/sensor2.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )
      // todo 2.keyBy之前 设置waterMark和指定数据中的timeStamp
//      .assignAscendingTimestamps(data => data.timestamp * 1000L) // todo 升序时间戳 数据是没有乱序  指定毫秒时间戳
      .assignTimestampsAndWatermarks(  // todo 有界的乱序程度可以确定的 过一段时间生成一个waterMark不是每条数据之后都带waterMark 适用于短时间内的密集数据
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {  // todo Time.seconds(3)是参数 最大乱序程度  waterMark在设置时就是当前最大EventTimeStamp - 3s  这个参数 给大了性能低  给小了结果不正确
          override def extractTimestamp(element: SensorReading): Long = {
            element.timestamp * 1000L // todo 时间戳
          }
        }
      )
    dataStream.print("dataStream")

    // 测输出流定义
    val lateTag: OutputTag[(String, Long, Double)] = new OutputTag[(String, Long, Double)]("late")

    // todo 3 用keyBy + window + allowedLateness + sideOutputLateData聚合 可以得到滚动窗口或者滑动窗口 比如得到最近一段时间内的结果 而不是从一开始到当前时间
    val resultStream = dataStream
      .map(data => (data.id, data.timestamp, data.temperature))
      .keyBy(data => data._1)  // todo keyBy这里是对元组操作 所以不能用“id” 可以用0 也可以lambda表达式
      .window(TumblingEventTimeWindows.of(  // todo 滚动 EventTime的开窗方法  滚动
        Time.seconds(10),  // window_size
        Time.seconds(0) // offset  正数是向后偏移 比如原来window=190-200变成191-201
      ))
//      .window(SlidingEventTimeWindows.of( // todo 滑动EventTime的开窗方法
//        Time.seconds(10), // window_size
//        Time.seconds(5), // sliding
//        Time.seconds(0) // offset
//      ))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) // todo 会话窗口 两个数据间隔>=10s 两条数据就是属于两个窗口
      .allowedLateness(Time.seconds(15))  // todo 4. 处理迟到数据  如果waterMark设置的较小 这里需要允许处理迟到数据
      .sideOutputLateData(lateTag) // todo 5. 侧输出流  处理迟到数据也不能无限等 无限等性能太差了 这里可以测输出流可以兜底
      .reduce( // 滚动窗口为10s  返回每个sensor 滚动窗口之内的最小温度值
        (curState, newData) => {
          (curState._1, newData._2,curState._3.min(newData._3))
        }
      )
    resultStream.getSideOutput(lateTag).print("late")
    resultStream.print("timeWindow1")
    /* todo 这里理解waterMark 迟到数据 测输出流  非常重要！！！
    // todo 起始的窗口开启点  timeStamp - (timeStamp - offset + windowSize) % windowSize = 199 - 9 = 190 窗口是190-200  假如offset=1  那就是191-201
    dataStream> SensorReading(sensor_1,1547718199,35.8)
    dataStream> SensorReading(sensor_2,1547718201,15.4)
    dataStream> SensorReading(sensor_3,1547718202,6.7)
    dataStream> SensorReading(sensor_4,1547718203,38.1) // todo 203-waterMark >= 200 关闭[190,200)window
    timeWindow1> (sensor_1,1547718199,35.8)
    dataStream> SensorReading(sensor_1,1547718206,34.8)
    dataStream> SensorReading(sensor_1,1547718208,31.8)
    dataStream> SensorReading(sensor_2,1547718209,21.8)
    dataStream> SensorReading(sensor_1,1547718211,14.8)
    dataStream> SensorReading(sensor_3,1547718212,24.7)
    dataStream> SensorReading(sensor_4,1547718214,24.9) // todo 关闭[200,210)window
    timeWindow1> (sensor_3,1547718202,6.7)
    timeWindow1> (sensor_1,1547718208,31.8)
    timeWindow1> (sensor_2,1547718209,15.4)
    timeWindow1> (sensor_4,1547718203,38.1)
    dataStream> SensorReading(sensor_1,1547718215,15.8)
    dataStream> SensorReading(sensor_1,1547718198,37.1)
    // todo 198是属于[190,200)window的 allowedLateness设置的是15s, 目前最新的数据timeStamp是215对应的waterMark是213 213-200 还是在迟到范围内的
    timeWindow1> (sensor_1,1547718198,35.8) // todo 最为迟到数据 timeStamp直接取最新的该数据timeStamp  temp与之前窗口聚合结果再比较 取较小值
    dataStream> SensorReading(sensor_1,1547718197,7.1) // 同理 输出190-200的 目前的waterMark是213 按迟到数据处理
    timeWindow1> (sensor_1,1547718197,7.1) // 更新了temp
    dataStream> SensorReading(sensor_4,1547718218,17.8)
    dataStream> SensorReading(sensor_1,1547718196,5.1)
    // todo 196是属于190-200的  目前的waterMark=218-3=215  215-200>=15超出了等待的范围 用侧输出流兜底 直接输出该数据
    late> (sensor_1,1547718196,5.1)
    dataStream> SensorReading(sensor_1,1547718222,11.8)
    dataStream> SensorReading(sensor_2,1547718223,14.8) // todo 关闭[210, 220)
    timeWindow1> (sensor_1,1547718215,14.8)
    timeWindow1> (sensor_3,1547718212,24.7)
    timeWindow1> (sensor_4,1547718218,17.8)
    dataStream> SensorReading(sensor_2,1547718224,14.9)
    dataStream> SensorReading(sensor_3,1547718226,15.7)
    dataStream> SensorReading(sensor_4,1547718231,19.8)
    dataStream> SensorReading(sensor_2,1547718232,32.1)
    dataStream> SensorReading(sensor_3,1547718233,33.2) // todo 关闭[220, 230)
    timeWindow1> (sensor_1,1547718222,11.8)
    timeWindow1> (sensor_2,1547718224,14.8)
    timeWindow1> (sensor_3,1547718226,15.7)
    dataStream> SensorReading(sensor_1,1547718234,13.4)
    dataStream> SensorReading(sensor_1,1547718193,36.8)
    late> (sensor_1,1547718193,36.8) // 侧输出流兜底
    dataStream> SensorReading(sensor_1,1547718193,5.8)
    late> (sensor_1,1547718193,5.8) // 侧输出流兜底
     */


    env.execute("waterMark")


  }
}
