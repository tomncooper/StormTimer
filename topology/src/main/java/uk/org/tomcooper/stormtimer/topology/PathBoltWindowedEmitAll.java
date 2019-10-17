package uk.org.tomcooper.stormtimer.topology;

import com.google.gson.Gson;
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
import uk.org.tomcooper.tracer.metrics.CPULatencyTimer;
import uk.org.tomcooper.tracer.metrics.TracerMetricManager;

import java.util.*;

public class PathBoltWindowedEmitAll extends BaseWindowedBolt {

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

	public PathBoltWindowedEmitAll(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}

	public PathBoltWindowedEmitAll() {
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
		name = context.getThisComponentId();

		windowLatency = new ReducedMetric(new MeanReducer());

		Long mWin = (Long) stormConf.get("topology.builtin.metrics.bucket.size.secs");
		int metricWindow = mWin.intValue();

		context.registerMetric("window-execute-latency", windowLatency, metricWindow);
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		long startTimeMs = System.currentTimeMillis();

		List<Tuple> inputs = inputWindow.get();

		List<PathMessage> pathMessages = new ArrayList<>();

		for (Tuple input : inputs) {

			tracer.addTransfer(input, startTimeMs - input.getLongByField("timestamp"));

			// Deserialise the path message and add it to the list
			Gson gson = new Gson();
			PathMessage pathMsg = gson.fromJson(input.getStringByField("pathMessage"), PathMessage.class);
			pathMessages.add(pathMsg);
		}

		// Now we reemmit each tuple so we have accurate path latencies
        // We are looping twice to create some extra processing latency
		for(PathMessage pathMessage : pathMessages) {

			pathMessage.addPathElement(name + ":" + taskID);

			Gson gson = new Gson();
			String pathMessageStr = gson.toJson(pathMessage);

			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, pathMessage.getOriginTimestamp(), pathMessageStr);
			collector.emit(outputStreamName, outputTuple);
		}

		for (Tuple input : inputs) {
			collector.ack(input);
		}

		tracer.addCPULatency(inputs.get(random.nextInt(inputs.size())), cpuTimer.stopTimer());
		// Update the window execute latency
		double winExLatencyMs = System.currentTimeMillis() - startTimeMs;
		windowLatency.update(winExLatencyMs);
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
}
