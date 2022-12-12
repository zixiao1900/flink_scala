package com.atguigu.flink

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// todo ProcessFunction最底层的操作  process()

object Flink023_ProcessFunction {
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

    // todo 需求 连续10秒内都是上升的数据 就报警
    val warningStream: DataStream[String] = dataStream
      .keyBy(data => data.id)
      .process(new TempIncreaseWarning(10000))

    warningStream.print()
    /*  todo 用的是processingTime  每2s来一条数据
    SensorReading(sensor_1,1547718199,35.8)
    SensorReading(sensor_2,1547718201,15.4)
    SensorReading(sensor_3,1547718202,6.7)
    SensorReading(sensor_4,1547718203,38.1)
    SensorReading(sensor_1,1547718206,34.8)
    SensorReading(sensor_1,1547718208,31.8)
    传感器sensor_2的温度连续10秒连续上升
    SensorReading(sensor_2,1547718209,21.8)
    SensorReading(sensor_1,1547718211,14.8)
    传感器sensor_3的温度连续10秒连续上升
    传感器sensor_4的温度连续10秒连续上升
    SensorReading(sensor_3,1547718212,24.7)
    SensorReading(sensor_4,1547718214,24.9)
    SensorReading(sensor_1,1547718215,15.8)
    SensorReading(sensor_1,1547718198,37.1)
    传感器sensor_2的温度连续10秒连续上升
    SensorReading(sensor_1,1547718197,7.1)
    SensorReading(sensor_4,1547718218,17.8)
    传感器sensor_3的温度连续10秒连续上升
    SensorReading(sensor_1,1547718196,5.1)
    SensorReading(sensor_1,1547718222,11.8)
    SensorReading(sensor_2,1547718223,14.8)
    SensorReading(sensor_2,1547718224,14.9)
    SensorReading(sensor_3,1547718226,15.7)
    SensorReading(sensor_4,1547718231,19.8)
    传感器sensor_1的温度连续10秒连续上升
    SensorReading(sensor_2,1547718232,32.1)
    SensorReading(sensor_3,1547718233,33.2)
    传感器sensor_2的温度连续10秒连续上升
    SensorReading(sensor_1,1547718234,13.4)
    SensorReading(sensor_1,1547718193,36.8)
    传感器sensor_4的温度连续10秒连续上升
    SensorReading(sensor_1,1547718193,5.8)
    传感器sensor_3的温度连续10秒连续上升

     */





    env.execute("processFunction")
  }
}

// todo 实现自定义KeyedProcessFunction
// 第一个值或者出现温度上升时，注册定时器  定时器还没出发前温度就出现下降了就删除该定时器  定时器触发时发出报警
class TempIncreaseWarning(interval: Int) extends KeyedProcessFunction[String, SensorReading, String] {
  // todo 定义状态 保存上一个温度值进行比较   保存注册定时器的时间戳用于删除  ValueState默认初始值是0.0
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp", classOf[Double])
  )
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer-ts", classOf[Long])
  )

  // todo 每条数据来 都会用到这个方法
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出状态
    val lastTemp: Double = lastTempState.value()
    val timerTs = timerTsState.value()

    val ts = ctx.timerService().currentProcessingTime() + interval // processingTime + 10s
    // 判断当前温度值和上次温度进行比较
    if (value.temperature > lastTemp && timerTs == 0) {
      //todo 如果温度上升 且timerTs为0说明没有定时器(要么第一条数据 要么之前状态被clear了) 那么注册当前数据时间戳10s之后的定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 更新状态
      timerTsState.update(ts)
      lastTempState.update(value.temperature)
    }
    else if (value.temperature < lastTemp) {
      // 如果温度下降了,那么删除定时器
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear() // 清空状态
      lastTempState.update(value.temperature)
    }
  }

  // todo 触发报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 触发输出
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval/1000 + "秒连续上升")
    timerTsState.clear() // 触发报警之后就清空
  }
}



// todo 并没有用在代码中 只是学习测试
// todo 继承KeyedProcessFunction  KeyedProcessFunction本身也是继承了富函数 可以调用运行时上下文和生命周期
class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String]{ // key类型，input类型，output类型

  // todo 1 可以做状态编程
  var myValueState: ValueState[Int] = _
  // lazy修饰的可以写在open方法外面
  lazy val myMapState: MapState[String, Boolean] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Boolean]("mapState", classOf[String], classOf[Boolean])
  )

  override def open(parameters: Configuration): Unit = {
    myValueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("myValueState", classOf[Int])
    )
  }


  // todo 必须实现的方法  value是每条数据 ctx是上下文  输出用out.collect()  可以不输出
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // todo 2. 可以获取waterMark 注册定时器 侧输出流
    ctx.getCurrentKey // 获取keyBy的key  也可以通过value.id得到
    ctx.timestamp() // 获取当前数据打上的时间戳  也可以通过value.timestamp得到
    ctx.timerService().currentWatermark() // todo 获取当前的waterMark
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L) // todo 注册定时器(可以注册多个)  当前时间戳+60s  到时间再触发
//    ctx.timerService().deleteEventTimeTimer() // 删除定时器操作 需要用状态存定时器开始的时间
    //    ctx.output() // todo 侧输出流的操作
  }

  // todo 注册的定时器 触发的操作  注册了多个定时器 都是在这里触发  需要通过timestamp参数判断是哪个定时器触发了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}



