package uk.org.tomcooper.stormtimer.topology;

import java.util.*;

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

public class JoinSplitBolt extends BaseWindowedBolt {
		
	protected OutputCollector collector;
	protected TracerMetricManager tracer;
	protected CPULatencyTimer cpuTimer;
	protected int taskID;
	protected String name;
	private transient ReducedMetric windowLatency;
	private String outStream1Name;
	private String outStream2Name;
	private Random random;
	private KeyGenerator keyGen;

	public JoinSplitBolt(String outStream1Name, String outStream2Name) {
		this.outStream1Name = outStream1Name;
		this.outStream2Name = outStream2Name;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tracer = new TracerMetricManager(stormConf, context);
		cpuTimer = new CPULatencyTimer();
		taskID = context.getThisTaskId();
		name = context.getComponentId(taskID);
		random = new Random();
		keyGen = new KeyGenerator();
		
		windowLatency = new ReducedMetric(new MeanReducer());
		
		Long mWin = (Long) stormConf.get("topology.builtin.metrics.bucket.size.secs");
        int metricWindow = mWin.intValue();
		context.registerMetric("window-execute-latency", windowLatency, metricWindow);
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		cpuTimer.startTimer(Thread.currentThread().getId());
		long startTimeMs = System.currentTimeMillis();
		long startTime = System.nanoTime();

		List<Tuple> inputs = inputWindow.get();

		Map<String, Map<String, List<PathMessage>>> streamMsgs = new HashMap<String, Map<String,List<PathMessage>>>();

		Gson gson = new Gson();

		// Sort the window path messages into streams
		for(Tuple input: inputs) {

			tracer.addTransfer(input, startTimeMs - input.getLongByField("timestamp"));

			// Deserialise the path message
			PathMessage pathMsg = gson.fromJson(input.getStringByField("pathMessage"), PathMessage.class);

			String inputStream = input.getSourceStreamId();

			Map<String, List<PathMessage>> pathMsgsMap;
			if(streamMsgs.containsKey(inputStream)){
				pathMsgsMap = streamMsgs.get(inputStream);
			} else {
				pathMsgsMap = new HashMap<>();
			}

			String pathStr = String.join(" ", pathMsg.getPath());
			List<PathMessage> pathMsgsList;
			if (pathMsgsMap.containsKey(pathStr)){
				pathMsgsList = pathMsgsMap.get(pathStr);
			} else {
				pathMsgsList = new ArrayList<>();
			}
			pathMsgsList.add(pathMsg);
			pathMsgsMap.put(pathStr, pathMsgsList);
			streamMsgs.put(inputStream, pathMsgsMap);
		}


		for(String streamID : streamMsgs.keySet()){
			// Now we create an output tuple by choosing a path at random
			List<String> pathKeys = new ArrayList<String>(streamMsgs.get(streamID).keySet());
			String randomPathKey = pathKeys.get(random.nextInt(pathKeys.size()));

			List<PathMessage>  chosenPathList = streamMsgs.get(streamID).get(randomPathKey);
			long totalTs = 0;
			for (PathMessage pMsg: chosenPathList ){
				totalTs += pMsg.getOriginTimestamp();
			}
			long avgEntryTs = totalTs / chosenPathList.size();

			// Choose a path message at random from all those that followed the chosen path
			PathMessage chosenPathMsg = chosenPathList.get(random.nextInt(chosenPathList.size()));

			// Add the current task to the path within the path message
			String newPathElement = name + ":" + taskID;
			chosenPathMsg.addPathElement(newPathElement);

			// Set the entry timestamp to the new value
			chosenPathMsg.setOriginTimestamp(avgEntryTs);
			String newPathMessageStr = gson.toJson(chosenPathMsg);

			String key = keyGen.chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, avgEntryTs, newPathMessageStr);

			collector.emit(outStream1Name, outputTuple);
			collector.emit(outStream2Name, outputTuple);
		}

		// Update the window execute latency
        double winExLatencyMs = (System.nanoTime()- startTime) / 1000000.0;
		windowLatency.update(winExLatencyMs);
	}		
	
	private boolean allEvenOrOdd(Map<String, Integer> input) {
		Collection<Integer> values = input.values();
		
		boolean allEven = true;
		boolean allOdd = true;
		
		for(int value: values) {
			if(value % 2 == 0) {
				// value is even
				allOdd = false;

			} else {
				// value is odd
				allEven = false;
			}
		}
		
		return (allEven || allOdd);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outStream1Name, new Fields("timestamp", "key", "entryMilliTimestamp", "pathMessage"));
		declarer.declareStream(outStream2Name, new Fields("timestamp", "key", "entryMilliTimestamp", "pathMessage"));
	}
		

}
