package com.atguigu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.util.Random


object Flink006_source_DIY {
  def main(args: Array[String]): Unit = {
    // 流任务环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)


    // TODO 自定义Source
    // todo 随机生成数据
    val DIYDataStream: DataStream[SensorReading] = env.addSource(new MysensorSource())
//    DIYDataStream.print()
    /*  顺序不能保证
    7> SensorReading(sensor_4,1670140351518,4.380313658968285)
    5> SensorReading(sensor_2,1670140351518,5.3108686528317515)
    4> SensorReading(sensor_1,1670140351518,97.5224557860865)
    6> SensorReading(sensor_3,1670140351518,43.941792482973476)
    8> SensorReading(sensor_1,1670140353523,97.43031543858181)
    10> SensorReading(sensor_3,1670140353523,43.27955458683303)
    9> SensorReading(sensor_2,1670140353523,5.046666030333535)
    11> SensorReading(sensor_4,1670140353523,3.995828160602533)
     */

    // todo 从文件中按顺序每次读1条或几条数据
    val DIYDataStreamFromFile: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt"))
    DIYDataStreamFromFile.print()
    /*  顺序不能保证
    9> sensor_2,1547718201,15.4
    8> sensor_1,1547718199,35.8
    10> sensor_3,1547718202,6.7
    11> sensor_4,1547718205,38.1
    12> sensor_1,1547718206,34.8
    1> sensor_1,1547718208,31.8
    2> sensor_2,1547718209,21.8
    3> sensor_1,1547718211,14.8
     */


    // todo 流处理执行
    env.execute("sourceDIY")
  }
}


case class SensorReading(id: String, timestamp: Long, temperature: Double)


// todo 随机生成数据
// 自定义MySensorSource                     泛型就是最后返回的类型
class MysensorSource() extends SourceFunction[SensorReading] {
  // todo 定义一个标识位flag 用来表示数据源是否正常发送数据
  var running: Boolean = true

  // todo 定义随机数发生器
  val rand = new Random()

  // 随机生成4个传感器的初始温度 (id, temp)  rand.nextDouble()随机生成0-1的Double数
  var curTemp: immutable.Seq[(String, Double)] = 1.to(4).map(i => ("sensor_" + i, rand.nextDouble() * 100))

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个无线循环 不停的产生数据 除非被cannel掉
    while (running) {
      // 在上次数据基础上 微调跟新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳加入数据中
      val curTimeStamp: Long = System.currentTimeMillis()
      curTemp.foreach(
        data => {
        val sensor: SensorReading = SensorReading(data._1, curTimeStamp, data._2)
        // todo 调用ctx.collect发出数据
        ctx.collect(sensor)
        }
      )
      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }


}

// todo 从文件中按行 每次出1条或多条数据 当作数据流
class MysensorSourceFromFile(dataPath:String) extends SourceFunction[String] {
  // todo 定义一个标识位flag 用来表示数据源是否正常发送数据
  var running: Boolean = true


  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    // 需要读取的文件
    val source: BufferedSource = Source.fromFile(dataPath)
    val lines: Iterator[String] = source.getLines()

    // 定义一个无线循环 不停的产生数据 除非被cannel掉
    while (running) {

      val tmpList: ListBuffer[String] = readData(lines)
      tmpList.foreach(
        (data: String) => ctx.collect(data)
      )


      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  // 从文件中读取1行或几行
  def readData(lines: Iterator[String]) = {
    val list: ListBuffer[String] = ListBuffer[String]()
    var num = new Random().nextInt(2) + 1 //  1 - 2条
    while (lines.hasNext && num > 0) {
      list.append(lines.next())
      num -= 1
    }
    list
  }
}

// todo 从文件中按行 每次出1条 当作数据流
class MysensorSourceFromFile1Line(dataPath:String) extends SourceFunction[String] {
  // todo 定义一个标识位flag 用来表示数据源是否正常发送数据
  var running: Boolean = true


  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    // 需要读取的文件
    val source: BufferedSource = Source.fromFile(dataPath)
    val lines: Iterator[String] = source.getLines()

    // 定义一个无线循环 不停的产生数据 除非被cannel掉
    while (running) {

      val tmpList: ListBuffer[String] = readData(lines)
      tmpList.foreach(
        (data: String) => ctx.collect(data)
      )


      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  // 从文件中读取1行
  def readData(lines: Iterator[String]) = {
    val list: ListBuffer[String] = ListBuffer[String]()
    if (lines.hasNext) {
      list.append(lines.next())
    }
    list
  }
}





