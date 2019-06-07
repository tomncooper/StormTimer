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
	private int multiplier;	
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private KeyGenerator keyGen;

	public PathBoltMultiplier(int multiplier) {
		this.multiplier = multiplier;
		keyGen = new KeyGenerator();
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);
	}
	

	@Override
	public void execute(Tuple input) {
		
		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));
		String pathMessage = PathMessageBuilder.createPathMessageStr(name, taskID, input);

		long entryNanoTimestamp = input.getLongByField("entryNanoTimestamp");
		long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");

		for(int i = 0; i < multiplier; i++) {		
			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, entryNanoTimestamp, entryMilliTimestamp, pathMessage);
			collector.emit("pathMessages", input, outputTuple);
		}

		collector.ack(input);
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("pathMessages", new Fields("timestamp", "key", "entryNanoTimestamp", "entryMilliTimestamp", "pathMessage"));
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
