package br.com.devmos.ecommercekafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		 KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
		 
		 String key = "123";
		 String value = "132123, 67543, 7897533";
		 
		 ProducerRecord<String, String> record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

		 
		 Callback callback = (data, ex) ->{
			 if(ex != null) {
				 ex.printStackTrace();
				 return;
			 }
			 System.out.println("Sucesso enviando " +data.topic() + ":::partition " + data.partition() + "/ offset " +data.offset());
			 
		 };
		 
		 String email = "Thank you for your order! We are processing your order!";
		 ProducerRecord<String, String> emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);
		 
		 
		producer.send(record, callback).get();
		producer.send(emailRecord, callback).get();
		 
	}
	
	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}

}
