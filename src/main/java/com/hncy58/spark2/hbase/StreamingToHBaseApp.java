package com.hncy58.spark2.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.ServerStatusReportUtil;

public class StreamingToHBaseApp {

	private static final Logger log = LoggerFactory.getLogger(StreamingToHBaseApp.class);

	private static transient final AtomicLong incr = new AtomicLong(0);
	
	public static String ZK_SERVERS = "162.16.6.180,162.16.6.181,162.16.6.182";
	public static String ZK_PORT = "2181";

	public static String KAFKA_SERVERS = "162.16.6.180:9092,162.16.6.181:9092,162.16.6.182:9092";
	private static String TOPIC_NAME = "test-topic-3";
	private static String KAFKA_GROUP = "KafkaToHBaseGroup";

	private static String SPARK_APP_NAME = StreamingToHBaseApp.class.getSimpleName();
	private static String SPARK_MASTER = "local[*]";

	private static int BATCH_DURATION = 5;

	private static String agentSvrName = "kafka_to_hbase_1";
	private static String agentSvrGroup = "KafkaToHBaseGroup";
	private static int agentSvrType = 2;
	private static int agentSourceType = 2;
	private static int agentDestType = 2;

public static void main(String[] args) {
		
		log.info("ARGS:KAFKA_SERVERS TOPIC_NAME KAFKA_GROUP ZK_SERVERS ZK_PORT SPARK_APP_NAME SPARK_MASTER BATCH_DURATION");
		log.info("eg:{} {} {} {} {} {} {} {}", KAFKA_SERVERS, TOPIC_NAME, KAFKA_GROUP, ZK_SERVERS, ZK_PORT, SPARK_APP_NAME, SPARK_MASTER, BATCH_DURATION);

		if (args.length > 0) {
			KAFKA_SERVERS = args[0].trim();
		}

		if (args.length > 1) {
			TOPIC_NAME = args[1].trim();
		}

		if (args.length > 2) {
			KAFKA_GROUP = args[2].trim();
		}
		
		if (args.length > 3) {
			ZK_SERVERS = args[3].trim();
		}

		if (args.length > 4) {
			ZK_PORT = args[4].trim();
		}

		if (args.length > 5) {
			SPARK_APP_NAME = args[5].trim();
		}

		if (args.length > 6) {
			SPARK_MASTER = args[6].trim();
		}

		if (args.length > 7) {
			BATCH_DURATION = Integer.parseInt(args[7].trim());
		}

		SparkConf conf = new SparkConf();
		conf.setMaster(SPARK_MASTER);
		conf.setAppName(SPARK_APP_NAME);
		conf.set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅关闭服务
				.set("spark.sql.crossJoin.enabled", "true").set("spark.driver.userClassPathFirst", "true")
		// .set("spark.streaming.kafka.maxRatePerPartition", "50000")
		;

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_SERVERS);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", KAFKA_GROUP);
		kafkaParams.put("auto.offset.reset", "latest"); // default latest
		kafkaParams.put("enable.auto.commit", false); // default true
		kafkaParams.put("auto.commit.interval.ms", 2000); // default 5000
		kafkaParams.put("isolation.level", "read_committed");

		Collection<String> topics = Arrays.asList(TOPIC_NAME);

		// 初始化参数，注册服务等
		init();

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(BATCH_DURATION));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// 处理数据，更新/插入到HBase表
		stream.foreachRDD(rdd -> {
			// 更新服务状态
			boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 1, "上报心跳");
			log.error("上报服务状态：" + ret);

			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			if (rdd.isEmpty())
				return;
			try {
				long sourceCnt = rdd.count();
				rdd.foreachPartition(it -> {
					List<Put> listPut = new ArrayList<Put>();
					it.forEachRemaining(r -> {
						Put put = new Put(Bytes.toBytes(r.key()));
						put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), r.timestamp(),
								Bytes.toBytes(r.value()));
						put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ts"), r.timestamp(),
								Bytes.toBytes(r.timestamp()));
						listPut.add(put);
					});

					Configuration hbaseConf = HBaseConfiguration.create();
					hbaseConf.set("hbase.zookeeper.quorum", ZK_SERVERS);
					hbaseConf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
					hbaseConf.set("hbase.defaults.for.version.skip", "true");
					Connection hbaseConn = null;
					Table table = null;
					try {
						hbaseConn = ConnectionFactory.createConnection(hbaseConf);
						table = hbaseConn.getTable(TableName.valueOf("test"));
						table.put(listPut);
					} finally {
						table.close();
						hbaseConn.close();
					}
				});
				// 更新无异常后提交日志偏移量
				((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
				log.error("total:" + incr.addAndGet(sourceCnt));
			} catch (Exception e) {
				log.error(e.getMessage(), e);
				ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
						"流处理捕获到异常:" + e.getMessage());
				log.info("上报告警结果：" + ret);
			} finally {

			}
		});
		try {
			jssc.start();
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}

		// 更新服务状态
		try {
			boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup, agentSvrType, 0, "检测服务中断信号，更新为下线状态");
			log.info("更新服务状态下线：" + ret);
		} catch (Exception e1) {
			log.error(e1.getMessage(), e1);
		}
	}

	private static void init() {

		log.info("usage:" + StreamingToHBaseApp.class.getName()
				+ " kafkaServers kafkaTopicGroupName kafkaToipcs FETCH_MILISECONDS MIN_BATCH_SIZE MIN_SLEEP_CNT SLEEP_SECONDS");
		log.info("eg:" + StreamingToHBaseApp.class.getName()
				+ " localhost:9092 kafka_hdfs_group_2 test-topic-1 1000 5000 3 5");

		int ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
				agentDestType);

		while (ret != 1) {
			log.error("注册服务失败，name:{}, group:{}, svrType:{}, sourceType:{}, destType:{}, 注册结果:{}", agentSvrName,
					agentSvrGroup, agentSvrType, agentSourceType, agentDestType, ret);
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
					agentDestType);
		}

		log.info("注册代理服务结果(-1:fail, 1:success, 2:standby) -> {}", ret);
	}
}
