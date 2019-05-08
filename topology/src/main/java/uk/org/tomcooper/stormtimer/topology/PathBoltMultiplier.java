package uk.org.tomcooper.stormtimer.topology;

import java.util.Random;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PathBoltMultiplier extends PathBolt {

	private static final long serialVersionUID = 5721514422535907702L;
	private static String[] keys = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"};
	private static Double[] weights = {(6.0/32.0), (4.0/32.0), (3.0/32.0), (3.0/32.0), (2.0/32.0), (2.0/32.0),  
								       (2.0/32.0), (2.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0), 
								       (1.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0)};
	private Random random;
	private Double[] cumulativeWeights;
	private int multiplier;
	
	public PathBoltMultiplier(int multiplier) {
		super();
		random = new Random();
		this.multiplier = multiplier;
	
		cumulativeWeights = new Double[weights.length];
		cumulativeWeights[0] = weights[0];
		for(int i = 1; i < cumulativeWeights.length; i++){
			cumulativeWeights[i] = cumulativeWeights[i-1] + weights[i];
		}
		
	}
	
	private String chooseKey() {
		
		double rand = random.nextDouble();
		for(int i = 0; i < cumulativeWeights.length; i++) {
		
			if(cumulativeWeights[i] >= rand) {
				return keys[i];
			}
			
		}
		return "A";
		
	}

	@Override
	public void execute(Tuple input) {
		
		cpuTimer.startTimer(Thread.currentThread().getId());
		tracer.addTransfer(input, System.currentTimeMillis() - input.getLongByField("timestamp"));
		String pathMessage = super.createPathMessage(input);


		for(int i = 0; i < multiplier; i++) {		
			String key = chooseKey();
			Values outputTuple = new Values(System.currentTimeMillis(), key, pathMessage);
			collector.emit("pathMessages", input, outputTuple);
		}

		collector.ack(input);
		tracer.addCPULatency(input, cpuTimer.stopTimer());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("pathMessages", new Fields("timestamp", "key", "pathMessage"));
	}
	
}
