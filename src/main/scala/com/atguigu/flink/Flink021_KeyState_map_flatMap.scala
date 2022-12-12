package com.atguigu.flink

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Map
import java.{lang, util}

// todo KeyState 状态编程

object Flink021_KeyState_map_flatMap {
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

    // todo 需求 对于温度传感器 温度跳变超过10度报警
    val alertSteam = dataStream
      .keyBy(data => data.id)
      .flatMap(
        new TempChangeAlert(10.0)
      )

    alertSteam.print()
    /* todo 根据sensor_id分组 组内温度变化超过10度就报警
    SensorReading(sensor_1,1547718199,35.8)
    SensorReading(sensor_2,1547718201,15.4)
    SensorReading(sensor_3,1547718202,6.7)
    SensorReading(sensor_4,1547718203,38.1)
    SensorReading(sensor_1,1547718206,34.8)
    SensorReading(sensor_1,1547718208,31.8)
    SensorReading(sensor_2,1547718209,21.8)
    SensorReading(sensor_1,1547718211,14.8)
    (sensor_1,31.8,14.8)
    SensorReading(sensor_3,1547718212,24.7)
    (sensor_3,6.7,24.7)
    SensorReading(sensor_4,1547718214,24.9)
    (sensor_4,38.1,24.9)
    SensorReading(sensor_1,1547718215,15.8)
    SensorReading(sensor_1,1547718198,37.1)
    (sensor_1,15.8,37.1)
    SensorReading(sensor_1,1547718197,7.1)
    (sensor_1,37.1,7.1)
    SensorReading(sensor_4,1547718218,17.8)
    SensorReading(sensor_1,1547718196,5.1)
    SensorReading(sensor_1,1547718222,11.8)
    SensorReading(sensor_2,1547718223,14.8)
    SensorReading(sensor_2,1547718224,14.9)
    SensorReading(sensor_3,1547718226,15.7)
    SensorReading(sensor_4,1547718231,19.8)
    SensorReading(sensor_2,1547718232,32.1)
    (sensor_2,14.9,32.1)
    SensorReading(sensor_3,1547718233,33.2)
    (sensor_3,15.7,33.2)
    SensorReading(sensor_1,1547718234,13.4)
    SensorReading(sensor_1,1547718193,36.8)
    (sensor_1,13.4,36.8)
    SensorReading(sensor_1,1547718193,5.8)
    (sensor_1,36.8,5.8)


     */


    env.execute("State_flatMap")
  }
}

// todo 实现自定义RichFlatmapFunction
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // todo 定义每个sensor_id给一个boolean值  默认值是false  接收到第一条数据之后设置为true
  lazy val mapState: MapState[String, Boolean] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Boolean]("mapState", classOf[String], classOf[Boolean])
  )

  // todo 定义状态保存上一次的温度值  默认值是0.0
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp", classOf[Double])
  )

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    val sensor_id = value.id
    // todo 不是该sensor_id的第一条数据 走这里
    if (mapState.get(sensor_id)) {  // todo mapState默认初始值是false 不会进来判断  如果mapState是true 就进来判断是否超过阈值

      // todo 从状态中获取上一次(最近一次)的温度值
      val lastTemp: Double = lastTempState.value()
      // todo 跟最新温度求差值最比较  变动大于threshold就报警
      val diff: Double = (value.temperature - lastTemp).abs
      if (diff > threshold) {
        // todo 触发报警条件 用out.collect输出  没有触发就不输出
        out.collect((value.id, lastTemp, value.temperature))
      }

      // todo 更新状态   当前温度更新为最近一次温度值
      lastTempState.update(value.temperature)
    }
    else { // todo 该sensor_id第一条数据来了 就将mapState设置为true 将lastTempState设置为第一条数据的温度
      mapState.put(sensor_id, true)
      lastTempState.update(value.temperature)
    }
  }
}


// 这里是了解 代码里并没有使用MyRichMapper
// todo Keyed state 必须定义在RichFunction中  因为需要运行时上下文 getRuntimeContext
// todo 继承富函数                           输入类型        输出类型
class MyRichMapper1 extends RichMapFunction[SensorReading, String] {
  // 声明值类型状态
  var valueState: ValueState[Double] = _
  // 列表类型状态
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("listState", classOf[Int])
  )
  // Map类型状态
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double])
  )
  // reduce类型状态
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[SensorReading](
      "reducingState",
      new MyReduceFunction, // 自定义的聚合函数   extends ReduceFunction
      classOf[SensorReading])
  )

  override def open(parameters: Configuration): Unit = {
    // todo 值类型状态  这里必须写在open里面 写在open里面之后 map方法又调用不到了 所以在外面声明了
    valueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("valueState", classOf[Double])
    )
  }


  override def map(value: SensorReading): String = {
    // todo valueStat
    // 读取状态
    val myValueState = valueState.value()
    // 更改写入状态
    valueState.update(value.temperature)

    // todo ListState
    listState.add(1) // 加一个数

    val JavaList = new util.ArrayList[Int]()
    JavaList.add(2)
    JavaList.add(3)
    listState.addAll(JavaList)  // 追加JavaList

    listState.update(JavaList)  // 替换为JavaList

    val iterable: lang.Iterable[Int] = listState.get() // 获取当前listState的值 得到可迭代类型

    // todo MapState
    mapState.contains("sensor_1") // 是否包含key
    mapState.get("sensor_1") // 拿到value
    mapState.put("sensor_1", 1.3)  // 更新操作
    val items: lang.Iterable[util.Map.Entry[String, Double]] = mapState.entries() // 得到k-v对的迭代器

    // todo reduceState
    reduceState.get() // 聚合完成的值
    reduceState.add(value) // 新假如的value和原先的聚合结果再聚合起来

    value.id
  }
}