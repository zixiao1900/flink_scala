package com.atguigu.flink

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.sql.{Connection, DriverManager}
import java.sql

object Flink017_sink_MySQL {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 文件模拟数据流  一次性读取 有界流 可以写入
//    val inputStream: DataStream[String] = env.readTextFile("datas/sensor1.txt") // todo batch一次性读取
    // todo 写入无界流的DataStream
    val inputStream: DataStream[String] = env.addSource(new MysensorSourceFromFile("datas/sensor1.txt"))

    // 传为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )


    // todo 没跑起来 报错 No suitable driver found for jdbc:mysql://localhost:3306/test01
    dataStream.addSink(
      new MyJdbcSinkFunc()
    )

    /* todo
    mysql中 use test01; select * from sensor_temp;
    +----------+------+
    | id       | temp |
    +----------+------+
    | sensor_1 | 14.8 |
    | sensor_3 | 24.7 |
    | sensor_2 | 21.8 |
    | sensor_4 | 38.1 |
    +----------+------+
    不断的查询 temp会不断根据流式数据进行更新
     */




    env.execute("sink_MySQL")
  }
}


// todo 继承富函数 可以获取运行时上下文和生命周期  建立连接的时候不需要每1来一条数据调用一次 在open和close中开启和关闭连接
class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
  // 定义连接  预编译语句
  var conn: Connection = _
  var insertStmt: sql.PreparedStatement = _
  var updateStmt: sql.PreparedStatement = _

  // open语句中建立连接
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(
      // todo ?useSSL=false&allowPublicKeyRetrieval=true 不加就报错
      "jdbc:mysql://localhost:3306/test01?useSSL=false&allowPublicKeyRetrieval=true",
      "root",
      "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading): Unit = {
    // 先执行更新操作 查到就更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 如果更新没有查到数据 那么就插入+
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1 ,value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
  // close中关闭连接
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}


// todo 连接mysql
// mysql -u root -p   password输入 123456
// mysql> use test01  // 切数据库
// 建表sensor_temp
/*
create table sensor_temp (
id varchar(20) not null,
temp double not null);
 */

// 运行程序
// 查表 select * from sensor_temp
