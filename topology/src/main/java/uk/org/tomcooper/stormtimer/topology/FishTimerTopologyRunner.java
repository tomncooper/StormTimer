package uk.org.tomcooper.stormtimer.topology;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

public class FishTimerTopologyRunner {

	public static void main(String[] args) {

		boolean async = false;
		if (args[2].equals("sync")) {
			async = false;
		} else if (args[2].equals("async")) {
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
		int multiplierMin = 1;
		int multiplierMax = 20;
		int multiplierMean = 10;
		double multiplierSTD = 1.0;

		String spoutName = "TimerSpout";
		builder.setSpout(spoutName, new TimerSpout(kafkaServer, groupID, incomingTopic), 2).setNumTasks(numTasks);

		String pathBoltName = "PathBolt";
		String pathBoltOutputStream = "Stream1";
		builder.setBolt(pathBoltName, new PathBolt(pathBoltOutputStream), 2).setNumTasks(numTasks)
				.shuffleGrouping(spoutName, "kafkaMessages");

		String pathMultiplierName = "PathMultiplier";
		String pathMultiplierOutputStream = "Stream2";
		builder.setBolt(pathMultiplierName,
				new PathBoltMultiplier(pathMultiplierOutputStream, multiplierMin, 
										multiplierMax, multiplierMean, multiplierSTD), 2)
				.setNumTasks(numTasks).shuffleGrouping(spoutName, "kafkaMessages");
		
	
		String joinSplitName = "JoinSpitBolt";
		String jsOutStream1 = "Stream3";
		String jsOutStream2 = "Stream4";
		Duration joinWindowLength = new Duration(2, TimeUnit.SECONDS);
        BaseWindowedBolt joinSplit = new JoinSplitBolt(jsOutStream1, jsOutStream2).withTumblingWindow(joinWindowLength);
		builder.setBolt(joinSplitName, joinSplit).setNumTasks(numTasks)
				.shuffleGrouping(pathBoltName, pathBoltOutputStream)
				.shuffleGrouping(pathMultiplierName, pathMultiplierOutputStream);
		
		String senderBoltAName = "SenderBoltA";
		builder.setBolt(senderBoltAName, new SenderBolt(kafkaServer, outgoingTopic, async), 2).setNumTasks(numTasks)
				//.fieldsGrouping(joinSplitName, jsOutStream1, new Fields("key"));
				.shuffleGrouping(joinSplitName, jsOutStream1);

		String senderBoltBName = "SenderBoltB";
		builder.setBolt(senderBoltBName, new SenderBolt(kafkaServer, outgoingTopic, async), 2).setNumTasks(numTasks)
				//.fieldsGrouping(joinSplitName, jsOutStream2, new Fields("key"));
				.shuffleGrouping(joinSplitName, jsOutStream2);

		StormTopology topology = builder.createTopology();

		if (args[0].equals("local")) {

			int numWorkers = 1;

			Config conf = BasicTimerTopologyRunner.createConf(false, numWorkers, numTasks, metricsBucketPeriod);
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

			Config conf = BasicTimerTopologyRunner.createConf(false, numWorkers, numTasks, metricsBucketPeriod);

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