package com.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class EventProducer {
    private static final Logger logger = LoggerFactory.getLogger(EventProducer.class);
    

    private void start(Context context) {
    	
		/** Producer properties **/
		Properties props = new Properties();
		props.put("metadata.broker.list", context.getString(EventSourceConstant.BROKER_LIST));
		props.put("serializer.class", context.getString(EventSourceConstant.SERIALIZER));
		props.put("request.required.acks", context.getString(EventSourceConstant.REQUIRED_ACKS));

		ProducerConfig config = new ProducerConfig(props);
		
		// Create kafka producer
		final Producer<String, String> producer = new Producer<String, String>(config);

		//final Map<String, String> headers = new HashMap<String, String>();

		String csvFile = "data/sorted100M.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		Event event;
		Gson g = new GsonBuilder().create();
		
		try {
	 
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				logger.info("Event readed");
			    // use comma as separator
				String[] array = line.split(cvsSplitBy);
				
				event = new Event();
				event.setId(Integer.parseInt(array[0]));
				event.setTimestamp(Integer.parseInt(array[1]));
				event.setValue(Double.parseDouble(array[2]));
				event.setProperty(Integer.parseInt(array[3]));
				event.setPlug_id(Integer.parseInt(array[4]));
				event.setHousehold_id(Integer.parseInt(array[5]));
				event.setHouse_id(Integer.parseInt(array[6]));	
				
				String s = g.toJson(event);
				
				// Send it to kafka brokers
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("smart.grid", s);
	            producer.send(data);
				logger.info(s);
			}
	 
		} catch (FileNotFoundException e) {
			logger.error("Error: "+e.getMessage());
		} catch (IOException e) {
			logger.error("Error: "+e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("Error al cerrar fichero csv");
				}
			}
		}
	 
    }
    
    public static void main(String[] args) {
		try {
		    Context context = new Context(args[0]);
		    EventProducer ep = new EventProducer();
		    ep.start(context);
		} catch (Exception e) {
		    logger.info(e.getMessage());
		}
    }
}