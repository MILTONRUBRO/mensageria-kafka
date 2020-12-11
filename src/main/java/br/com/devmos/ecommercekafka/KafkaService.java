package br.com.devmos.ecommercekafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService {
	
	private final KafkaConsumer<String, String> kafkaConsumer;
	private final ConsumerFunction parse;

	public KafkaService(String topic, ConsumerFunction parse) {
		this.parse = parse;
		this.kafkaConsumer = new KafkaConsumer<String, String>(properties());
		kafkaConsumer.subscribe(Collections.singletonList(topic));

	}

	public void run() {
		
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

			if (!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " registros");
				
				for (ConsumerRecord record : records) {
					parse.consume(record);
				}
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
