package uk.org.tomcooper.stormtimer.topology;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;

import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

public class SenderBolt implements IRichBolt {

	private static final long serialVersionUID = -3583539827595476015L;
	private OutputCollector collector;
	private TracerMetricManager tracer;
	private CPULatencyTimer cpuTimer;
	private int taskID;
	private String name;
	private KafkaProducer<String, String> kafkaProducer;
	private Properties kafkaProperties;
	private String topic;
	private boolean async;

	public SenderBolt(String kafkaServer, String topic, boolean async) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaServer);
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		kafkaProperties = props;
		this.topic = topic;
		this.async = async;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);

		kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

	}

	@Override
	public void execute(Tuple input) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));

		Gson gson = new Gson();
		PathMessage pathMsg = gson.fromJson(input.getStringByField("pathMessage"), PathMessage.class);
		String newPathElement = name + ":" + taskID;
		pathMsg.addPathElement(newPathElement);	
		long nanoLatencyNs = System.nanoTime() - pathMsg.getEntryNanoTimestamp();
		long milliLatency = System.currentTimeMillis() - pathMsg.getEntryMilliTimestamp();
		double nanoLatencyMs = ((double) nanoLatencyNs) / 1000000.0;
		pathMsg.setStormNanoLatencyMs(nanoLatencyMs);
		pathMsg.setStormMilliLatencyMs(milliLatency);

		String pathMessage = gson.toJson(pathMsg);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, pathMessage);
		Future<RecordMetadata> result = kafkaProducer.send(record);
		if (!async) {
			try {
				result.get();
				collector.ack(input);
			} catch (ExecutionException | InterruptedException err) {
				collector.reportError(err);
				collector.fail(input);
			}
		} else {
			collector.ack(input);
		}
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
