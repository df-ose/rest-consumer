package com.respuesta.Consumer;



import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;




import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;




@SpringBootApplication
@Configuration
@EnableKafka

public class Consumidor {



	 @Value("${broker1}")
     private String broker1;
	 @Value("${port1}")
	 private String port1;
	 @Value("${broker2}")
     private String broker2;
	 @Value("${port2}")
	 private String port2;
	 @Value("${broker3}")
     private String broker3;
	 @Value("${port3}")
	 private String port3;	 
	 @Value("${topico1}")
     private String topico1;


	private  final Logger LOG = LoggerFactory.getLogger("Consumer-rest-App");

	Properties props1 = new Properties();
	Properties props2 = new Properties();
	Properties props3 = new Properties();

	private String topic="";
	

	private String groupidclient;

	private String datasend;

	private void configuracion1(Properties config) {

        config.put("bootstrap.servers", broker1+":"+port1+","+broker2+":"+port2+","+broker3+":"+port3);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //config.put("fetch.min.bytes", 1);
        config.put("group.id",groupidclient);
        //config.put("heartbeat.interval.ms", 3000);
        //config.put("max.partition.fetch.bytes", 1048576);
        //config.put("session.timeout.ms", 30000);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //config.put("connections.max.idle.ms", 540000);
        //config.put("enable.auto.commit", true);
        //config.put("exclude.internal.topics", true);
        //config.put("max.poll.records", 2147483647);
        //config.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        //config.put("request.timeout.ms", 40000);
        //config.put("auto.commit.interval.ms", 5000);
        //config.put("fetch.max.wait.ms", 500);
        //config.put("metadata.max.age.ms", 300000);
        //config.put("reconnect.backoff.ms", 50);
        //config.put("retry.backoff.ms", 100);
        config.put("client.id","rest-1");


	}


	private  void configuracion2(Properties config) {

	    config.put("bootstrap.servers", broker1+":"+port1+","+broker2+":"+port2+","+broker3+":"+port3);
	    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    config.put("group.id",groupidclient);
	    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    config.put("client.id","rest-2");

		}


	private  void configuracion3(Properties config) {

	    config.put("bootstrap.servers", broker1+":"+port1+","+broker2+":"+port2+","+broker3+":"+port3);
	    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    config.put("group.id",groupidclient);
	    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    config.put("client.id","rest-3");

		}


	private void removeLastChar(String string)
	{

	StringBuffer sb= new StringBuffer(string);

	sb.deleteCharAt(sb.length()-1);

	this.datasend=sb.toString();

	}

	 //@Autowired
	 //ObjectMapper objectMapper;


	public  String ejecutar() throws IOException, JsonProcessingException  {


		   ObjectMapper objectMapper = new ObjectMapper();

		   JsonNode jsondata1;

		   //JsonNode registrojson;

		   //opciones();

		  
		   
		   
		   

		   groupidclient=UUID.randomUUID().toString();

	       String name ;
		   String lastname ;
		   String function ;
		   String email ;
		   String city ;

		   int contador = 0 ;

	       //mensaje = "";

	       datasend = "";

	       
	       
	       
	       //topic="usuarios-topic" ;
	       topic=topico1;
	       
	       configuracion1(props1);

		   KafkaConsumer<String, String> myConsumer1 = new KafkaConsumer<String, String>(props1);

	 	    // Create a topic subscription list:
	       ArrayList<TopicPartition> partition1 = new ArrayList<TopicPartition>();
	        //partitions.add(new TopicPartition("topico_prueba1", 0)); // Adds a TopicPartition instance representing a topic and a partition.
	       partition1.add(new TopicPartition(topic, 0)); // Adds an additional TopicPartition instance representing a different partition within the topic. Change as desired.
	       //partitions.add(new TopicPartition("usuarios", 1));
	       //partitions.add(new TopicPartition("usuarios", 2));
	        // Assign partitions to the Consumer:
	       myConsumer1.assign(partition1);

	         //Retrieves the topic subscription list from the SubscriptionState internal object:
	       //Set<TopicPartition> assignedPartitions = myConsumer.assignment();

	       //ArrayList<String> resp = new ArrayList<String>();
			//resp.add(records.value());
	       // Start polling for messages:
           try {

	           ConsumerRecords<String, String> record1 = myConsumer1.poll(Duration.ofMillis(500));

	           for(ConsumerRecord<String, String> records : record1)

	           {

	    	   //System.out.printf("terminado %s %d %d ",records.offset(), records.key(), records.value());
	           //System.out.printf("terminado  ",records.offset(), records.key(), records.value());



			   jsondata1  = objectMapper.readTree(records.value());

			   //registrojson = jsondata1.get("registro");


			   //datasend =  datasend +"{\"" +jsondata1.get("eventId").asText()+"\":"+ registrojson.toPrettyString()+"},";

			   datasend =  datasend +jsondata1.toPrettyString()+",";



			   jsondata1=null;
			   //registrojson=null;

			   //name = registrojson.get("name").asText();
			   //lastname = registrojson.get("lastname").asText();
			   //function = registrojson.get("function").asText();
			   //email = registrojson.get("email").asText();
			   //city = registrojson.get("city").asText();
		    	//datasend = datasend + "{\"name\":\""+name+"\",\"lastname\":\""+lastname+"\",\"function\":\""+function+"\",\"email\":\""+email+"\",\"city\":\""+city+"\"}"  ;




	           }


	       }

           finally {
	            myConsumer1.close();
	        }


           //return resp.stream().collect(Collectors.toList()).toString();

           configuracion2(props2);
           KafkaConsumer<String, String> myConsumer2 = new KafkaConsumer<String, String>(props2);

	 	    // Create a topic subscription list:
	       ArrayList<TopicPartition> partition2 = new ArrayList<TopicPartition>();
	       //partitions.add(new TopicPartition("topico_prueba1", 0)); // Adds a TopicPartition instance representing a topic and a partition.
	       //partition2.add(new TopicPartition("usuarios", 0)); // Adds an additional TopicPartition instance representing a different partition within the topic. Change as desired.
	       partition2.add(new TopicPartition(topic, 1));
	       //partitions.add(new TopicPartition("usuarios", 2));
	       // Assign partitions to the Consumer:
	       myConsumer2.assign(partition2);

	       //Retrieves the topic subscription list from the SubscriptionState internal object:
	       //Set<TopicPartition> assignedPartitions = myConsumer.assignment();
   	       // Start polling for messages:
          try {

	           ConsumerRecords<String, String> record2 = myConsumer2.poll(Duration.ofMillis(500));

	           for(ConsumerRecord<String, String> records : record2)

	           {

	    	   //System.out.printf("terminado %s %d %d ",records.offset(), records.key(), records.value());
	           //  System.out.printf("terminado  ",records.offset(), records.key(), records.value());


	    	    //System.out.printf(records.value()  );

				jsondata1  = objectMapper.readTree(records.value());

		        //registrojson = jsondata1.get("registro");

		        //datasend =  datasend +"{\"" +jsondata1.get("eventId").asText()+"\":"+ registrojson.toPrettyString()+"},";

				datasend =  datasend +jsondata1.toPrettyString()+",";

			    jsondata1=null;
			    //registrojson=null;


				//name = registrojson.get("name").asText();
				//lastname = registrojson.get("lastname").asText();
				//function = registrojson.get("function").asText();
				//email = registrojson.get("email").asText();
				//city = registrojson.get("city").asText();

				//datasend = datasend + "{\"name\":\""+name+"\",\"lastname\":\""+lastname+"\",\"function\":\""+function+"\",\"email\":\""+email+"\",\"city\":\""+city+"\"}"  ;

	    	   //System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", records.topic(), records.partition(), records.offset(), records.key(), records.value()));


	           }


	       }

          finally {
	            myConsumer2.close();
	        }


          //return resp.stream().collect(Collectors.toList()).toString();




          configuracion3(props3);
          KafkaConsumer<String, String> myConsumer3 = new KafkaConsumer<String, String>(props3);

	 	    // Create a topic subscription list:
	       ArrayList<TopicPartition> partition3 = new ArrayList<TopicPartition>();
	       //partitions.add(new TopicPartition("topico_prueba1", 0)); // Adds a TopicPartition instance representing a topic and a partition.
	       //partition2.add(new TopicPartition("usuarios", 0)); // Adds an additional TopicPartition instance representing a different partition within the topic. Change as desired.
	       //partition3.add(new TopicPartition("usuarios", 1));
	       partition3.add(new TopicPartition(topic, 2));
	        // Assign partitions to the Consumer:
	       myConsumer3.assign(partition3);

	         //Retrieves the topic subscription list from the SubscriptionState internal object:
	       //Set<TopicPartition> assignedPartitions = myConsumer.assignment();


	       // Start polling for messages:
         try {

	           ConsumerRecords<String, String> record3 = myConsumer3.poll(Duration.ofMillis(500));

	           for(ConsumerRecord<String, String> records : record3)

	           {

	    	   //System.out.printf("terminado %s %d %d ",records.offset(), records.key(), records.value());
	           //  System.out.printf("terminado  ",records.offset(), records.key(), records.value());


				jsondata1  = objectMapper.readTree(records.value());

		        //registrojson = jsondata1.get("registro");

		        //datasend =  datasend +"{\"" +jsondata1.get("eventId").asText()+"\":"+ registrojson.toPrettyString()+"},";

				datasend =  datasend +jsondata1.toPrettyString()+",";


			    jsondata1=null;
			    //registrojson=null;


				//name = registrojson.get("name").asText();
				//lastname = registrojson.get("lastname").asText();
				//function = registrojson.get("function").asText();
				//email = registrojson.get("email").asText();
				//city = registrojson.get("city").asText();

				//datasend = datasend + "{\"name\":\""+name+"\",\"lastname\":\""+lastname+"\",\"function\":\""+function+"\",\"email\":\""+email+"\",\"city\":\""+city+"\"}"  ;
	    	   //System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", records.topic(), records.partition(), records.offset(), records.key(), records.value()));



	           }


	       }

         finally {
	            myConsumer3.close();
	        }


	       removeLastChar(datasend);


	       datasend  =   "{\"respuesta\":["+datasend+"]}";


	       return datasend;




	    }

	
}