package uk.org.tomcooper.stormtimer.topology;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;

import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

public class PathAdderBolt implements IRichBolt {

	private static final long serialVersionUID = 9077261044025094494L;
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private KeyGenerator keyGen;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);
		keyGen = new KeyGenerator();
	}
	
	@Override
	public void execute(Tuple input) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));

		String pathMessage = input.getStringByField("pathMessage");
		
		Gson gson = new Gson();
		PathMessage pathMsg = gson.fromJson(pathMessage, PathMessage.class);

		String newPathElement = name + ":" + taskID;
		pathMsg.addPathElement(newPathElement);	
		String pathMessageStr = gson.toJson(pathMsg);

		String key = keyGen.chooseKey();

		long entryNanoTimestamp = input.getLongByField("entryNanoTimestamp");
		long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");

		Values outputTuple = new Values(System.currentTimeMillis(), key, entryNanoTimestamp, entryMilliTimestamp, pathMessageStr);

		collector.emit("pathAddedMessages", input, outputTuple);

		collector.ack(input);
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("pathAddedMessages", new Fields("timestamp", "key", "entryNanoTimestamp", "entryMilliTimestamp", "pathMessage"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
