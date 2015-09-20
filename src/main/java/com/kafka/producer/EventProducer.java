package com.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.*;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class EventProducer {
	private static final Logger logger = LoggerFactory
			.getLogger(EventProducer.class);

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

		Event event;
		Gson g = new GsonBuilder().create();
		int id = 1, timestamp = 1377986401;
		float max_value, min_value;
		int plug_id, household_id, house_id;
		int cuentaKey = 0;
		try {
			int cuenta = 0;
			String[] lista = new String[8];

			while (true) {

				if (id % 2500 == 0) {
					timestamp++;
				}
				Random random = new Random();

				house_id = random.nextInt(40);
				household_id = random.nextInt(14);
				plug_id = random.nextInt(14);

				max_value = random.nextFloat() * 300;
				min_value = random.nextFloat() * 300;

				if (max_value < min_value) {
					float aux = min_value;
					min_value = max_value;
					max_value = aux;
				}

				for (int i = 0; i < 2; i++) {
					event = new Event();
					event.setId(id);
					event.setTimestamp(timestamp);

					if (i == 0) {
						event.setValue(max_value);
					} else {
						event.setValue(min_value);
					}

					event.setProperty(i);

					event.setPlug_id(plug_id);
					event.setHousehold_id(household_id);
					event.setHouse_id(house_id);

					String s;
					s = g.toJson(event);

					lista[cuenta] = s;
					cuenta++;
					if (cuenta == 8) {
						cuenta = 0;
						String envio = "";
						for (int u = 0; u < lista.length; u++) {
							envio += lista[u] + "\n";
						}
						// Send it to kafka brokers
						KeyedMessage<String, String> data = new KeyedMessage<String, String>(
								topic, Integer.toString(cuentaKey), envio);
						producer.send(data);
						cuentaKey++;
						// logger.info(s);
					}
					id++;

				}

			}

		} catch (Exception e) {
			logger.error("Error: " + e.getMessage());
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