package uk.org.tomcooper.stormtimer.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

import java.util.Map;

public class SimplePathBoltMultiplier implements IRichBolt {

	private static final long serialVersionUID = -5499409182647305065L;
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private KeyGenerator keyGen;
	private int multiplier;
	private String outputStreamName;

	public SimplePathBoltMultiplier(String outputStreamName, int multiplier) {
	    this.multiplier = multiplier;
		this.outputStreamName = outputStreamName;
	}

	public SimplePathBoltMultiplier(int multiplier) {
		this("pathMessages", multiplier);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
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
		String pathMessage = PathMessageBuilder.createPathMessageStr(name, taskID, input);

		long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");

		for (int i = 0; i < multiplier; i++) {
			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, entryMilliTimestamp,
					pathMessage);

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
