package uk.org.tomcooper.stormtimer.topology;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
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

		Tuple randomSourceTuple = inputs.get(random.nextInt(inputs.size()));

		Map<Integer, List<PathMessage>> spoutPathMessages = new HashMap<>();
		Map<Integer, List<Long>> spoutEntryTimestamps = new HashMap<>();

		for (Tuple input : inputs) {

			tracer.addTransfer(input, startTimeMs - input.getLongByField("timestamp"));

			// Deserialise the path message so we can extract the spout information
			Gson gson = new Gson();
			PathMessage pathMsg = gson.fromJson(input.getStringByField("pathMessage"), PathMessage.class);

			int spoutTask = pathMsg.getSpoutTaskID();

			// Add the entry timestamp for this tuple to the list for the relevant source spout
			List<Long> spoutTsList = spoutEntryTimestamps.getOrDefault(spoutTask, new ArrayList<>());
			spoutTsList.add(input.getLongByField("entryMilliTimestamp"));
			spoutEntryTimestamps.put(spoutTask, spoutTsList);

			// Store the path message for later use
			List<PathMessage> spoutPathMessageList = spoutPathMessages.getOrDefault(spoutTask, new ArrayList<>());
			spoutPathMessageList.add(pathMsg);
			spoutPathMessages.put(spoutTask, spoutPathMessageList);
		}

		// Now we create an output tuple for each spout Task ID we have seen
		for(Integer spoutTask: spoutEntryTimestamps.keySet()) {

			// Average the entry timestamps for tuples from this spout task
		    List<Long> spoutEntryTs = spoutEntryTimestamps.get(spoutTask);
			long avgEntryTs = spoutEntryTs.stream().mapToLong(i->i).sum() / spoutEntryTs.size();

			// Choose a path at random from all the paths of tuple from this spout task
			List<PathMessage> spoutPathsList = spoutPathMessages.get(spoutTask);
			PathMessage newPathMsg = spoutPathsList.get(random.nextInt(spoutPathsList.size()));

			// Create new PathMessage
			newPathMsg.setOriginTimestamp(avgEntryTs);
			newPathMsg.addPathElement(name + ":" + taskID);

			// Serialise the new path message
			Gson gson = new Gson();
			String pathMessageStr = gson.toJson(newPathMsg);

			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, avgEntryTs, pathMessageStr);
			collector.emit(outputStreamName, outputTuple);
		}
		tracer.addCPULatency(randomSourceTuple, cpuTimer.stopTimer());

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
