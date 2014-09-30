package com.vip.wms.kafka;

public class KafkaDemo {

	public static void main(String[] args) {
		KafkaProducer producerThread = new KafkaProducer("this is my first kafka messages");
		producerThread.start();

		KafkaConsumer consumerThread = new KafkaConsumer("this is my first kafka messages");
		consumerThread.start();
	}
}
