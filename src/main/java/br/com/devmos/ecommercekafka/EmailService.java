package br.com.devmos.ecommercekafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class EmailService {

	public static void main(String[] args) {
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties());

		kafkaConsumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

			if (records.isEmpty()) {
				System.out.println("no records found");
			}

			for (ConsumerRecord record : records) {
				System.out.println("-----------------------------------------------------------------");
				System.out.println("Send Email");
				System.out.println(record.key());
				System.out.println(record.value());
				System.out.println(record.partition());
				System.out.println(record.offset());
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();

				}

				System.out.println("Email Sent");

			}
		}

	}

	private static Properties properties() {
		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());

		return properties;
	}

}