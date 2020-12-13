package br.com.devmos.ecommercekafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
	
	public static void main(String[] args) {
		
		FraudDetectorService fraudService = new FraudDetectorService();
		KafkaService service = new KafkaService(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_ORDER", fraudService::parse);
		
		service.run();
	}
	
	private void parse(ConsumerRecord<String, String> record) {
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
