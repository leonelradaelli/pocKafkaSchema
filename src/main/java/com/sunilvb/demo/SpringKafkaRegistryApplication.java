package com.sunilvb.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@SpringBootApplication
@RestController
public class SpringKafkaRegistryApplication {

	final static Logger logger = Logger.getLogger(SpringKafkaRegistryApplication.class);
	@Value("${bootstrap.url}")
	String bootstrap;
	@Value("${registry.url}")
	String registry;
	
	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaRegistryApplication.class, args);
			
	}
	@RequestMapping("/persona") 	// Url htpp que escuchara
	public String doIt(@RequestParam(value="topic", defaultValue="persona") String topic) // Parametros  nombre del topic
	{
		
		String ret=topic;
		System.out.println("Topic: " +ret);
		try
		{

			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;
			
			Properties properties = new Properties();
			// Kafka Properties
			properties.setProperty("bootstrap.servers", bootstrap);
			properties.setProperty("acks", "all");
			properties.setProperty("retries", "10");
			// Avro properties
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
			properties.setProperty("schema.registry.url", registry);
			
			ret += sendMsg(properties, topic);
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}
		
		return ret;
	}
	
	private Persona sendMsg(Properties properties, String topic)
	{
		// Se define un productor con las propiedades anteriores
		Producer<String, Persona> producer = new KafkaProducer<String, Persona>(properties);

		// Se crea una persona con datos hardcodeados (podrian venir por parametros, pero no es lo que nos interesa mostrar)
		Persona persona = Persona.newBuilder()// ES EL OBJETO PERSONA el que define la estructura en base a persona.avro
        		.setId("1")
        		.setNombre("Bill")
        		.setApellido("Gates")
                .build();

        ProducerRecord<String, Persona> producerRecord = new ProducerRecord<String, Persona>(topic, persona);

        
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info(metadata); 
                } else {
                	logger.error(exception.getMessage());
                }
            }
        });

        producer.flush();
        producer.close();
        
        return persona;
	}
	
	
}
