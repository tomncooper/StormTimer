package uk.org.tomcooper.stormtimer.topology;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class BasicTimerTopologyRunner {

	static Config createConf(boolean debug, int numWorkers, int maxTaskPar, int metricsBucketPeriod) {

		Config conf = new Config();
		conf.setDebug(debug);
		conf.setMaxTaskParallelism(maxTaskPar);

		conf.put("topology.builtin.metrics.bucket.size.secs", metricsBucketPeriod);

		//Set the disruptor flush interval and batch size
		//conf.put("topology.disruptor.batch.timeout.millis", 1000);
		//conf.put("topology.disruptor.batch.size", 100);

		// This is really important! Will need to see the effect on accuracy using a
		// lower sampling rate. It is unlikely
		// that you would run a sampling rate of 1.0 in a production environment due to
		// the overhead.
		conf.put("topology.stats.sample.rate", 1.0);

		// Disable backpressure as I have yet to see how it effects the predictions
		conf.put("topology.backpressure.enable", false);

		conf.put("tracerdb.host", "http://tracer.ukwest.cloudapp.azure.com:8086");
		conf.put("tracerdb.user", "tom");
		conf.put("tracerdb.password", "bigdata");
		conf.put("tracerdb.name", "tracer");

		conf.setNumWorkers(numWorkers);
		conf.registerMetricsConsumer(uk.org.tomcooper.tracer.metrics.Consumer.class, numWorkers);
		conf.put("topology.acker.executors", numWorkers);

		return conf;
	}

	public static void main(String[] args) {

		boolean async = false;
		if(args[2].equals("sync")){
			async = false;
		} else if (args[2].equals("async")){
			async = true;
		} else {			
			System.err.println("Invalid argument: " + args[2] + " should be 'sync' or 'async'");
			System.exit(1);
		}
		
		TopologyBuilder builder = new TopologyBuilder();

		String kafkaServer = "tncbroker.ukwest.cloudapp.azure.com:9092";
		String groupID = "spout_group";
		String incomingTopic = "beforeStorm";
		String outgoingTopic = "afterStorm";

		int numTasks = 16;
		int metricsBucketPeriod = 2;
		
		String spoutName = "TimerSpout";
		String spoutOutStreamName = "kafkaMessages";
		int numSpoutStreams = 1;
		builder.setSpout(spoutName, new TimerSpout(kafkaServer, groupID, incomingTopic, spoutOutStreamName, numSpoutStreams), 2).setNumTasks(numTasks);

		String pathBoltName = "PathBolt";
		builder.setBolt(pathBoltName, new PathBolt(), 2)
				.setNumTasks(numTasks)
				.shuffleGrouping(spoutName, spoutOutStreamName + "0");

		String senderBoltName = "SenderBolt";
		builder.setBolt(senderBoltName, new SenderBolt(kafkaServer, outgoingTopic, async), 2)
				.setNumTasks(numTasks)
				.shuffleGrouping(pathBoltName, "pathMessages");

		StormTopology topology = builder.createTopology();

		if (args[0].equals("local")) {
			
			int numWorkers = 1;

			Config conf = createConf(false, numWorkers, numTasks, metricsBucketPeriod);
			ILocalCluster cluster = new LocalCluster();

			System.out.println("\n\n######\nSubmitting Topology to Local " + "Cluster\n######\n\n");

			try {
				cluster.submitTopology(args[1], conf, topology);
			} catch (InvalidTopologyException ite) {
				System.err.println("\n######\nInvalid Topology Exception\n######\n");
				System.err.println("Error Message: " + ite.get_msg());
				System.err.println("\n\nStack Trace:");
				ite.printStackTrace();
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			}

			System.out.println("\n\n######\nTopology Submitted");
			System.out.println("Sleeping for " + args[3] + "ms....\n######\n\n");

			try {
				Thread.sleep(Long.valueOf(args[3]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			cluster.shutdown();
		} else if (args[0].equals("remote")) {

			int numWorkers = 4;

			Config conf = createConf(false, numWorkers, numTasks, metricsBucketPeriod);

			try {
				StormSubmitter.submitTopology(args[1], conf, topology);
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}

		} else {
			System.err.println("Invalid argument: " + args[0] + " should be 'local' or 'remote'");
		}

	}
}
