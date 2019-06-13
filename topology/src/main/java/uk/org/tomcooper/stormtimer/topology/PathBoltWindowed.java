package uk.org.tomcooper.stormtimer.topology;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import com.google.gson.Gson;

import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

public class PathBoltWindowed extends BaseWindowedBolt {

	private static final long serialVersionUID = -5499409182647305065L;
	private Random random;
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private transient ReducedMetric windowLatency;
	private KeyGenerator keyGen;
	private String outputStreamName;

	public PathBoltWindowed(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}

	public PathBoltWindowed() {
		this("pathMessages");
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		random = new Random();
		keyGen = new KeyGenerator();
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);

		windowLatency = new ReducedMetric(new MeanReducer());

		Long mWin = (Long) stormConf.get("topology.builtin.metrics.bucket.size.secs");
		int metricWindow = mWin.intValue();

		context.registerMetric("window-execute-latency", windowLatency, metricWindow);
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		long startTime = System.nanoTime();

		List<Tuple> inputs = inputWindow.get();

		Tuple randomSourceTuple = inputs.get(random.nextInt(inputs.size()));

		tracer.addTransfer(randomSourceTuple,
				System.currentTimeMillis() - randomSourceTuple.getLongByField("timestamp"));

		long nanoTotal = 0;
		long milliTotal = 0;

		for (Tuple input : inputs) {

			long entryNanoTimestamp = input.getLongByField("entryNanoTimestamp");
			long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");

			nanoTotal += entryNanoTimestamp;
			milliTotal += entryMilliTimestamp;
		}

		long avgNanoTimestamp = nanoTotal / inputs.size();
		long avgMilliTimestamp = milliTotal / inputs.size();

		String pathMessageStr;
		try {
			pathMessageStr = PathMessageBuilder.createPathMessageStr(name, taskID, randomSourceTuple);
		} catch(IllegalArgumentException err) {
			
			String pathMessage = randomSourceTuple.getStringByField("pathMessage");
			
			Gson gson = new Gson();
			PathMessage pathMsg = gson.fromJson(pathMessage, PathMessage.class);

			String newPathElement = name + ":" + taskID;
			pathMsg.addPathElement(newPathElement);	
			pathMessageStr = gson.toJson(pathMsg);
		}

		String key = keyGen.chooseKey();
		Values outputTuple = new Values(System.currentTimeMillis(), key, avgNanoTimestamp, avgMilliTimestamp,
				pathMessageStr);
		collector.emit(outputStreamName, outputTuple);
		tracer.addCPULatency(randomSourceTuple, cpuTimer.stopTimer());

		// Update the window execute latency
		double winExLatencyMs = (System.nanoTime() - startTime) / 1000000.0;
		windowLatency.update(winExLatencyMs);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStreamName,
				new Fields("timestamp", "key", "entryNanoTimestamp", "entryMilliTimestamp", "pathMessage"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
}
