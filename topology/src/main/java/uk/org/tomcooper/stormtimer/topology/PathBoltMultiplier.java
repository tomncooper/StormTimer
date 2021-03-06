package uk.org.tomcooper.stormtimer.topology;

import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

public class PathBoltMultiplier implements IRichBolt {

	private static final long serialVersionUID = -5499409182647305065L;
	private MultiplierGenerator generator;
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private KeyGenerator keyGen;
	private int min;
	private int max;
	private int mean;
	private double std;
	private String outputStreamName;

	public PathBoltMultiplier(String outputStreamName, int min, int max, int mean, double std) {
		this.min = min;
		this.max = max;
		this.mean = mean;
		this.std = std;
		this.outputStreamName = outputStreamName;

	}

	public PathBoltMultiplier(int min, int max, int mean, double std) {
		this("pathMessages", min, max, mean, std);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);
		generator = new MultiplierGenerator(min, max, mean, std);
		keyGen = new KeyGenerator();
	}

	@Override
	public void execute(Tuple input) {

		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));
		String pathMessage = PathMessageBuilder.createPathMessageStr(name, taskID, input);

		long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");
		int multiplier = generator.generateMultiplier();

		for (int i = 0; i < multiplier; i++) {
			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, entryMilliTimestamp, pathMessage);

			collector.emit(outputStreamName, input, outputTuple);
		}

		collector.ack(input);
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStreamName,
				new Fields("timestamp", "key", "entryMilliTimestamp", "pathMessage"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
