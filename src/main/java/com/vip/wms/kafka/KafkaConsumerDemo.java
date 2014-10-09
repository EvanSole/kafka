package com.vip.wms.kafka;

public class KafkaConsumerDemo {

	public static void main(String[] args) {
		KafkaConsumer consumerThread = new KafkaConsumer("将预订topics并消费消息的程序成为consumer.");
		consumerThread.start();
	}
}
