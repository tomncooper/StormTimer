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

public class PathBoltWindowedEmitOneRandom extends BaseWindowedBolt {

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
	private boolean createPathMsg;

	public PathBoltWindowedEmitOneRandom(String outputStreamName, boolean createPathMsg) {
		this.outputStreamName = outputStreamName;
		this.createPathMsg = createPathMsg;
	}

	public PathBoltWindowedEmitOneRandom(String outputStreamName) {
	    this(outputStreamName, false);
	}

	public PathBoltWindowedEmitOneRandom(boolean createPathMsg) {
		this("pathMessages", createPathMsg);
	}

	public PathBoltWindowedEmitOneRandom() {
		this("pathMessages", false);
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

		Map<String, List<PathMessage>> pathPathMessages = new HashMap<>();
		Map<String, List<Long>> pathEntryTimestamps = new HashMap<>();

		Gson gson = new Gson();

		for (Tuple input : inputs) {

			tracer.addTransfer(input, startTimeMs - input.getLongByField("timestamp"));

			// Deserialise the path message so we can extract the spout information
			String pathMsgStr;
			if (createPathMsg) {
				pathMsgStr = PathMessageBuilder.createPathMessageStr(name, taskID, input);
			} else {
				pathMsgStr = input.getStringByField("pathMessage");
			}
			PathMessage pathMsg = gson.fromJson(pathMsgStr, PathMessage.class);

			String path = pathMsg.getPath().toString();

			// Add the entry timestamp for this tuple to the list for the relevant path
			List<Long> pathTsList = pathEntryTimestamps.getOrDefault(path, new ArrayList<>());
			pathTsList.add(input.getLongByField("entryMilliTimestamp"));
			pathEntryTimestamps.put(path, pathTsList);

			// Store the path message for later use
			List<PathMessage> pathPathMessageList = pathPathMessages.getOrDefault(path, new ArrayList<>());
			pathPathMessageList.add(pathMsg);
			pathPathMessages.put(path, pathPathMessageList);
		}

		// Now we create an output tuple by choosing a path at random
		List<String> pathKeys = new ArrayList<String>(pathPathMessages.keySet());
		String randomPathKey = pathKeys.get(random.nextInt(pathKeys.size()));

		// Average the entry timestamps for tuples from this spout task
		List<Long> pathEntryTs = pathEntryTimestamps.get(randomPathKey);
		long avgEntryTs = pathEntryTs.stream().mapToLong(i->i).sum() / pathEntryTs.size();

		PathMessage randomPathMessage = pathPathMessages.get(randomPathKey).get(0);

		// Update the random PathMessage
		randomPathMessage.setOriginTimestamp(avgEntryTs);
		randomPathMessage.addPathElement(name + ":" + taskID);

		// Serialise the new path message
		String pathMessageStr = gson.toJson(randomPathMessage);

		String key = keyGen.chooseKey();
		Values outputTuple = new Values(System.currentTimeMillis(), key, avgEntryTs, pathMessageStr);
		collector.emit(outputStreamName, outputTuple);

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
