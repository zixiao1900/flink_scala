flink
001-002 从文件读取有界流无界流
003-004 从内存 文件 kafka中读取流式数据
005     从文件中生产流式数据到kafka生产者   后面很多都用到这个
006     从文件中生产流式数据直接到内存中
007     基本的keyBy + 聚合方法
008     keyBy + 自定义聚合方法
009     流式数据分流split
010     流式数据合流connect + coMap
011     filter
012     map
013-017 流式数据写入File, Kafka, Redis, ES, MySql
018-020 waterMark window EventTime
021-022 KeyState, RichFunction
023-024 ProcessFunction




netcat工具像端口不断发送数据 模拟数据流
cmd：nc -lp 9999
cmd: 输入一行一行的数据

// 获取端口数据
val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)
控制台没来一条数据 就打印一个结果

kafka相关操作 windows kafka闪退就清理日志

-- 启动zookeeper 不要关
cmd1: bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
-- 启动kafa  不要关
cmd2: bin/windows/kafka-server-start.bat ./config/server.properties

-- 创建topic
bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --topic kafka_sink_test01 --partitions 2 --replication-factor 1
return:
    Created topic test001.

-- 查看topic信息
bin/windows/kafka-topics.bat --list --bootstrap-server localhost:9092
return:
    test
    test001

-- 生产者 发送消息
cmd3: bin/windows/kafka-console-producer.bat --broker-list localhost:9092 --topic topic_flink_sensor
然后在cmd3中可以写消息发送

-- 接受消息
cmd4: bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic topic_flink_sensor
cmd4中就会接受到cmd3中发送的消息

-- 关闭的时候 windows任务管理器 详细信息里面java开头的都kill掉


redis相关操作
-- 启动redis
cmd1: redis-server 开启之后不要关闭
cmd2: redis-cli


查看所有key cmd2: keys *
运行Flink015_sink_Redis
查询表中的sensor_1对应的value: hget sensor_temp sensor_1  如果由多个sensor_1 返回最新的那个
查询表中所有的k-v: hgetall sensor_temp


elasticsearch相关操作
-- 下载 https://www.elastic.co/cn/downloads/past-releases/elasticsearch-6-4-0
-- 启动
cmd1: elasticsearch.bat
开打 http://localhost:9200/ 看到如下json就是启动成功
{
  "name" : "-tIm1UU",
  "cluster_name" : "elasticsearch",
  "cluster_uuid" : "XlPJyQjmS0GQxr2fR6RWYw",
  "version" : {
    "number" : "6.4.0",
    "build_flavor" : "default",
    "build_type" : "zip",
    "build_hash" : "595516e",
    "build_date" : "2018-08-17T23:18:47.308994Z",
    "build_snapshot" : false,
    "lucene_version" : "7.4.0",
    "minimum_wire_compatibility_version" : "5.6.0",
    "minimum_index_compatibility_version" : "5.0.0"
  },
  "tagline" : "You Know, for Search"
}


mysql相关
下载
https://dev.mysql.com/downloads/mysql/5.7.html
一开始配置mysql
https://blog.csdn.net/m0_59073956/article/details/125128430
https://www.jb51.net/article/216538.htm

-- 启动mysql cmd1: mysql -u root -p
        Enter password: 123456
mysql>  出现就是成功





