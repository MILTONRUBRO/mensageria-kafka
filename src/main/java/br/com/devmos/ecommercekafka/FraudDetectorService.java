package br.com.devmos.ecommercekafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudDetectorService {
	
	public static void main(String[] args) {
		
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties());
		
		kafkaConsumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
		
		while(true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			
			if(records.isEmpty()) {
				System.out.println("Nenhum registro encontrado");
			}
			
			for (ConsumerRecord record : records) {
				
				System.out.println("-----------------------------------------------------------------");
				System.out.println("Processsing new order, checking for fraude");
				System.out.println(record.key());
				System.out.println(record.value());
				System.out.println(record.partition());
				System.out.println(record.offset());

				try {
					Thread.sleep(5000);
				}catch(InterruptedException e) {
					e.printStackTrace();
					
				}
				
				System.out.println("Order processed");

			}
		}

	}

	private static Properties properties() {
		Properties properties = new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "_" + UUID.randomUUID().toString());

		return properties;
	}

}
