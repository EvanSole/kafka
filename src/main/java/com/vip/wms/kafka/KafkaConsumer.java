package com.vip.wms.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/***
 * 消费者
 * 
 * @author wenchang.wang
 * 
 */
public class KafkaConsumer extends Thread {

	private final ConsumerConnector consumer;
	private final String topic;

	private static ConsumerConfig createConsumerConfig(){
		Properties props = new Properties();
		try {
			props.load(ClassLoader.getSystemResourceAsStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		props.put("zookeeper.connect", props.getProperty("zookeeper.connect"));
		props.put("group.id", props.getProperty("group.id"));
		props.put("zookeeper.session.timeout.ms",props.getProperty("zookeeper.session.timeout.ms"));
		props.put("zookeeper.sync.time.ms",props.getProperty("zookeeper.sync.time.ms"));
		props.put("auto.commit.interval.ms",props.getProperty("auto.commit.interval.ms"));
		return new ConsumerConfig(props);
	}
	
	public KafkaConsumer(String topic){
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	
	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println("receive：" + new String(it.next().message()));
			try {
				sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
