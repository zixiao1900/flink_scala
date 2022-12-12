package com.atguigu.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


object Flink001_batch_wordCount {
  def main(args: Array[String]): Unit = {
    // todo flink 统计datas/words.txt 批处理wordCount

    // 对仅仅做对比  Spark
//    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
//    val sc: SparkContext = new SparkContext(sparConf)
    // spark读文件
//    val inputRDD: RDD[String] = sc.textFile("datas/words.txt")

    // todo flink批  类似 sc
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // todo flink读数据  DataSet 有界集   不需要sc.stop()
    val inputDataSet: DataSet[String] = env.readTextFile("datas/words.txt")

    // todo基于DataSet方法 对数据进行转换统计 先分词 再对word分组 最后聚合统计
    // todo 这个有必要写 必然flatMap都会报错
//    import org.apache.flink.api.scala._

    val wordCount: AggregateDataSet[(String, Int)] = inputDataSet
      .flatMap((line: String) => line.split(" "))
      .map(word => {
        (word, 1)
      })
      .groupBy(0) // idx 以idx_0 就是根据word分组
      .sum(1)

    wordCount.print()
    /*
    (spark,3)
    (hadoop,2)
    (scala,1)
    (hello,3)
     */

    // collect触发之后 又从头到尾执行了一遍
    val wordCountSeq: Seq[(String, Int)] = wordCount.collect()
    println(wordCountSeq) // Buffer((spark,3), (hadoop,2), (scala,1), (hello,3))


  }
}
