package com.kafka.producer;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class EventProducer {
	private static final Logger logger = LoggerFactory
			.getLogger(EventProducer.class);

	final String url = "http://luna1/datasets/sorted100M.csv.gz";

	// Return the same number of partitions as kafka brokers
	private int getPartitions(String brokers) {
		int partitions = 1;
		String[] brokers_array = brokers.split(",");
		partitions = brokers_array.length;

		return partitions;
	}

	private void start(Context context) {

		String topic = context.getString(EventSourceConstant.KAFKA_TOPIC);
		topic = topic.replace("\"", "");

		int sessionTimeoutMs = 10000;
		int connectionTimeoutMs = 10000;
		String zk_list = context.getString(EventSourceConstant.ZOOKEEPER_LIST);
		zk_list = zk_list.replace("\"", ""); // Remove " "
		String[] zk_array = zk_list.split(",");
		// Create a ZooKeeper client
		logger.info("Zookeeper: " + zk_array[0]);

		ZkClient zkClient = new ZkClient(zk_array[0], sessionTimeoutMs,
				connectionTimeoutMs, ZKStringSerializer$.MODULE$);

		// If topic don't exists must be created
		if (!AdminUtils.topicExists(zkClient, topic)) {

			int numPartitions = getPartitions(context
					.getString(EventSourceConstant.BROKER_LIST));
			int replicationFactor = 1;

			AdminUtils.createTopic(zkClient, topic, numPartitions,
					replicationFactor, new Properties());
			logger.info("topic created");
		} else {
			logger.info("topic not created");
		}

		/** Producer properties **/
		Properties props = new Properties();
		props.put("metadata.broker.list",
				context.getString(EventSourceConstant.BROKER_LIST));
		props.put("serializer.class",
				context.getString(EventSourceConstant.SERIALIZER));
		props.put("request.required.acks",
				context.getString(EventSourceConstant.REQUIRED_ACKS));
		props.put("producer.type",
				context.getString(EventSourceConstant.PRODUCER_TYPE));
		props.put("batch.num.messages",
				context.getString(EventSourceConstant.BATCH_SIZE));
		props.put("partitioner.class",
				"com.kafka.producer.RoundRobinPartitioner");

		ProducerConfig config = new ProducerConfig(props);

		// Create kafka producer
		final Producer<String, String> producer = new Producer<String, String>(
				config);

		ArrayList<String> events = new ArrayList<String>(100000000);

		URL urlObj;
		HttpURLConnection conn;
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		Event event;
		Gson g = new GsonBuilder().create();

		try {
			urlObj = new URL(url);
			conn = (HttpURLConnection) urlObj.openConnection();
			conn.setRequestMethod("GET");
			InputStream connis = conn.getInputStream();
			GZIPInputStream gzis = new GZIPInputStream(connis,
					connis.available());

			br = new BufferedReader(new InputStreamReader(gzis),
					connis.available());

			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] array = line.split(cvsSplitBy);

				event = new Event();
				event.setId(Integer.parseInt(array[0]));
				event.setTimestamp(Integer.parseInt(array[1]));
				event.setValue(Float.parseFloat(array[2]));
				event.setProperty(Integer.parseInt(array[3]));
				event.setPlug_id(Integer.parseInt(array[4]));
				event.setHousehold_id(Integer.parseInt(array[5]));
				event.setHouse_id(Integer.parseInt(array[6]));

				String s;
				s = g.toJson(event);

				events.add(s);

				// Send it to kafka brokers
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						topic, s);
				producer.send(data);
				// logger.info(s);
			}

		} catch (FileNotFoundException e) {
			logger.error("Error: " + e.getMessage());
		} catch (IOException e) {
			logger.error("Error: " + e.getMessage());
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