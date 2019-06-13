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
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;

public class AllInOneTimerTopologyRunner {

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
		int multiplierMin = 1;
		int multiplierMax = 20;
		int multiplierMean = 10;
		double multiplierSTD = 1.0;
		int metricsBucketPeriod = 2;

		String spoutName = "TimerSpout";
		builder.setSpout(spoutName, new TimerSpout(kafkaServer, groupID, incomingTopic), 2).setNumTasks(numTasks);

		String pathMultiplierName = "MultiPathBolt";
		String pathMultiplierOutputStream = "Stream2";
		builder.setBolt(pathMultiplierName, new PathBoltMultiplier(pathMultiplierOutputStream, multiplierMin,
				multiplierMax, multiplierMean, multiplierSTD), 2).setNumTasks(numTasks)
				.shuffleGrouping(spoutName, "kafkaMessages");

		Count windowCount = new Count(10);
		String pathWindowerName = "WindowedPathBolt";
		String pathWindowerOutputStream = "Stream3";
		builder.setBolt(pathWindowerName,
				new PathBoltWindowed(pathWindowerOutputStream).withTumblingWindow(windowCount), 2).setNumTasks(numTasks)
				.fieldsGrouping(pathMultiplierName, pathMultiplierOutputStream, new Fields("key"));

		String senderBoltName = "SenderBolt";
		builder.setBolt(senderBoltName, new SenderBolt(kafkaServer, outgoingTopic, async), 2).setNumTasks(numTasks)
				.fieldsGrouping(pathWindowerName, pathWindowerOutputStream, new Fields("key"));

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