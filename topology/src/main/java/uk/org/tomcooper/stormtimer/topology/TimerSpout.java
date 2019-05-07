package uk.org.tomcooper.stormtimer.topology;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TimerSpout implements IRichSpout {

	private static final long serialVersionUID = 4378833198055439673L;
	private SpoutOutputCollector collector;
	private transient KafkaConsumer<String, String> kafkaConsumer;
	private Properties kafkaProperties;
	private List<String> topics;
	private int taskID;
	private String name;

	public TimerSpout(String kafkaServer, String groupID, String topic) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaServer);
		props.setProperty("group.id", groupID);
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		kafkaProperties = props;
		topics = new ArrayList<String>();
		topics.add(topic);

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void activate() {
		kafkaConsumer = new KafkaConsumer<String, String>(kafkaProperties);
		kafkaConsumer.subscribe(topics);
	}

	public void deactivate() {
		kafkaConsumer.commitSync();
		kafkaConsumer.close();
	}

	public void nextTuple() {

		ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1));

		for (ConsumerRecord<String, String> record : records) {

			String uuid = record.value();
			long messageTimestamp = record.timestamp();
			String path = name + ":" + taskID;
			Values outputTuple = new Values(System.currentTimeMillis(), uuid, messageTimestamp, path);

			collector.emit("kafkaMessages", outputTuple, uuid);

		}

	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("kafkaMessages", new Fields("timestamp", "uuid", "messageTimestamp", "path"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
