package com.vip.wms.kafka;

import java.io.IOException;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/***
 * 生产者
 * 
 * @author wenchang.wang
 * 
 */
public class KafkaProducer extends Thread {

	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public KafkaProducer(String topic){
		try {
			props.load(ClassLoader.getSystemResourceAsStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		props.put("serializer.class", props.getProperty("serializer.class"));
		props.put("metadata.broker.list",props.getProperty("metadata.broker.list"));
		producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
		this.topic = topic;
	}

	@Override
	public void run() {
		int messageNo = 1;
		while (true) {
			String messageStr = new String("Message_" + messageNo);
			System.out.println("Send:" + messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			messageNo++;
			
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
