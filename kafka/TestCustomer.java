package kafka;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestCustomer {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		if(args.length<1){
			System.out.println("Not enough parameters!");
			System.exit(1);
		}
		
		String topicname = args[0];
	
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.143:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("session.timeout.ms", "30000");
		props.put("auto.commit.intervals.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		ArrayList<String> names =new ArrayList<>();
		names.add(topicname);
		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(names);

		// print the topic name
		//System.out.println("Subscribed to topic " + topicname);
		
		a: while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100000);
			for (ConsumerRecord<String, String> record : records)
				
				// print the offset,key and value for the consumer records.
				System.out.println(record.value());
				if(!isAlive(topicname)){
					break a ;
				}
		}
		//System.out.println(commons.tools.T()+"[INFO] Receiver "+ topicname+" finished. Total message received: "+ i);
	}
	public static boolean isAlive(String topic){
		boolean alive = true;
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		String day = topic.replaceAll("traffic", "");
		try {
			Date time  = f.parse(day);
			long tb = time.getTime();
			long tn = System.currentTimeMillis();
			if((tn - tb)/1000 > 87400){
				alive = false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return alive;
	}
}
