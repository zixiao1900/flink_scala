package com.atguigu.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._



object Flink002_stream_wordCount {
  def main(args: Array[String]): Unit = {
    // todo flink 统计datas/words.txt 流处理wordCount

    // todo args 传参  在Run/Edit Configrations/Program Arguments: --host localhost --port 9999 --numParam 1
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")
    val numPara = parameterTool.getInt("numParam")

    // todo flink流 类似 ssc
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    StreamExecutionEnvironment.createLocalEnvironment() // todo 本地开发环境其实是调用这个方法 会自动判断
//    StreamExecutionEnvironment.createRemoteEnvironment(jobManager-hostName, portNum, wordCount.jar) // todo 集群环境自动调用这个方法

    // todo 并行度 可以超出核数
//    env.setParallelism(numPara)

    // sparkStreaming做为对比
    // val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // todo 接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
    // todo 转换操作
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(line => line.split(" "))
      .filter(word => word.nonEmpty)
//      .setParallelism(4) // 可以在每个算子后面都可以设置并行度
      .map(word => (word, 1))
      .keyBy(0) // 类似groupby idx_0分组  idx_1 sum聚合
      .sum(1)


    resultDataStream.print()
      .setParallelism(numPara) // todo 写入文件的时候 并行度可以用1

    // input1: hello word hello flink
    // input2: hello spark hello hello
    // input3: word word hello
    /* todo 统计的是从接受到流到目前为止累加的wordCount 每接收一个word 显示到当前为止该word的一共count
    todo 前面的数字代表当前统计的并行的子任的序号 分布式计算
    output1:
    7> (hello,1)
    19> (flink,1)
    18> (word,1)
    7> (hello,2)
    output2:
    2> (spark,1)
    7> (hello,3)
    7> (hello,4)
    7> (hello,5)
    output3:
    7> (hello,6)
    18> (word,2)
    18> (word,3)
     */


    // 启动任务执行 类似ssc.start()
    env.execute("SteamWordCount")

  }
}

// todo
//  1 cmd: nc -lp 9999
//  2 启动程序
//  3 输入数据
//  4 手动退出也会退出 不然程序不会退出
