package uk.org.tomcooper.stormtimer.topology;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt implements IRichBolt{
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		
		System.out.println("Message: " + input.getStringByField("pathMessage"));
		collector.ack(input);
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
