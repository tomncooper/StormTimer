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

public class PathBolt implements IRichBolt {

	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);
	}
	
	protected String createPathMessage(Tuple input) {
		String path1 = input.getStringByField("path");
		String path2 = name + ":" + taskID;
		String[] path = { path1, path2 };
		String messageID = input.getStringByField("uuid");
		long originTimestamp = input.getLongByField("messageTimestamp");
		long entryTimestamp = input.getLongByField("entryTimestamp");

		PathMessage pathMsg = new PathMessage();
		pathMsg.setMessageID(messageID);
		pathMsg.setOriginTimestamp(originTimestamp);
		pathMsg.setPath(path);
		pathMsg.setEntryTimestamp(entryTimestamp);
		
		Gson gson = new Gson();
		String pathMessage = gson.toJson(pathMsg);

		return pathMessage;
	}

	@Override
	public void execute(Tuple input) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));

		String pathMessage = createPathMessage(input);

		Values outputTuple = new Values(System.currentTimeMillis(), pathMessage);

		collector.emit("pathMessages", input, outputTuple);

		collector.ack(input);
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("pathMessages", new Fields("timestamp", "pathMessage"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
