package uk.org.tomcooper.stormtimer.topology;

import java.util.Collection;
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
	private boolean simple;
	
	public JoinSplitBolt(String outStream1Name, String outStream2Name, boolean simple) {
		this.outStream1Name = outStream1Name;
		this.outStream2Name = outStream2Name;
		this.simple = simple;
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

		String oldPathMessageStr = inputs.get(random.nextInt(inputs.size())).getStringByField("pathMessage");	

		long nanoTotal = 0;
		long milliTotal = 0;
		
		Map<String, Integer> streamCounts = new HashMap<String, Integer>();

		for(Tuple input: inputs) {

			tracer.addTransfer(input, startTimeMs - input.getLongByField("timestamp"));
			
			long entryNanoTimestamp = input.getLongByField("entryNanoTimestamp");
			long entryMilliTimestamp = input.getLongByField("entryMilliTimestamp");
			
			nanoTotal += entryNanoTimestamp;
			milliTotal += entryMilliTimestamp;
			
			String inputStream = input.getSourceStreamId();
		    
			if(streamCounts.containsKey(inputStream)){
				streamCounts.put(inputStream, (streamCounts.get(inputStream) + 1));
			} else {
				streamCounts.put(inputStream, 1);
			}
			
			
		}
		
		long avgNanoTimestamp = nanoTotal / inputs.size();
		long avgMilliTimestamp = milliTotal / inputs.size();
		
        // Add the current task to the path within the path message
		Gson gson = new Gson();
		PathMessage pathMsg = gson.fromJson(oldPathMessageStr, PathMessage.class);
		String newPathElement = name + ":" + taskID;
		pathMsg.addPathElement(newPathElement);		
		String newPathMessageStr = gson.toJson(pathMsg);	

		String key = keyGen.chooseKey();
		Values outputTuple = new Values(System.currentTimeMillis(), key, avgNanoTimestamp, avgMilliTimestamp, newPathMessageStr);	

		if(simple) {
			collector.emit(outStream1Name, outputTuple);			
			collector.emit(outStream2Name, outputTuple);			
		} else {
			String outStream;
			if(allEvenOrOdd(streamCounts)) {
				outStream = outStream1Name;
			} else {			
				outStream = outStream2Name;
			}
			collector.emit(outStream, outputTuple);
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
		declarer.declareStream(outStream1Name, new Fields("timestamp", "key", "entryNanoTimestamp", "entryMilliTimestamp", "pathMessage"));
		declarer.declareStream(outStream2Name, new Fields("timestamp", "key", "entryNanoTimestamp", "entryMilliTimestamp", "pathMessage"));
	}
		

}
