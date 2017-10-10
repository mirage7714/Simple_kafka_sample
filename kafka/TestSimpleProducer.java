package kafka;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestSimpleProducer {
	static HashMap<Integer, String> traffic = new HashMap<>();
	
	public static void main(String [] args){
		/*
		if(args.length!= 2){
			System.out.println(commons.tools.T()+"[ERROR] Not enough parameters!!");
			System.out.println("Usage: <Input File> <Kafka server> ");
			System.exit(1);
		}
		*/
		long tb = System.currentTimeMillis();
		//String file = args[0];
		//String server = args[1]+":9092";
		String file = "d:/201710080010.xml";
		String t = LoadData(file);
		String topicname = "traffic20171008";
		System.out.println(commons.tools.T()+"[INFO] Current topic: "+ topicname);
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.143:9092");
		props.put("acks", "all");
		props.put("retries", "0");
		props.put("linger.ms", "1");
		props.put("buffer.memory", "33554432");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String>producer = new KafkaProducer<String, String>(props);
		for(int n = 0;n<traffic.size();n++){
			producer.send(new ProducerRecord<String, String>(topicname, Integer.toString(n),traffic.get(n)));
			//System.out.println("Message sent successfully");
		}
		producer.close();
		System.out.println(commons.tools.T()+"[INFO] Transfer Finished. Total message: "+ traffic.size()+". Total execute time: "+(System.currentTimeMillis()-tb)/1000.0+" s");
		
	}
	public static String LoadData(String file){
		String topic = "";
		BufferedReader read;
		int index = 0;
		try {
			read = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf8"));
			while(read.ready()){
				String line = read.readLine();
				if(line.contains("routeid")){
					String line1 = line.replaceAll("</Info>", "").replaceAll("<", "").replaceAll(">", "").trim();
					topic = line1.substring(line1.lastIndexOf("=")+1).replaceAll("\"","").replaceAll("/", "").split("\\s")[0];
					String [] conts = line1.replaceAll("\"", "").split("=");
					String id = conts[1].split("\\s")[0];
					String level = conts[2].split("\\s")[0];
					String speed = conts[3].split("\\s")[0];
					String traveltime = conts[4].split("\\s")[0];
					String create = conts[5];
					traffic.put(index, id+","+level+","+speed+","+traveltime+","+create);
					index++;
					
				}
				
			}
			read.close();
			System.out.println(commons.tools.T()+"[INFO] Total record: "+ index);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return topic;
	}
}
