package testStorm;

import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import graph.Edge;
import graph.GraphStream;
import graph.QueryGraph;

public class testdefault {
	public static void main(String[] args) {
		String HomeDir = "d:/install/MarsJava/testStorm/";
		String querypath = HomeDir + "testdat/Q1.txt";
		String streampath = HomeDir + "testdat/G1.txt";
		QueryGraph Qgraph = new QueryGraph(querypath);
		GraphStream Gstream = new GraphStream(streampath);
		FeederSpout[] Espouts = Qgraph.getEdgeSpouts();
		
		TopologyBuilder pathTB = Qgraph.getLeftDeepTopology2();
		Config conf = new Config();
		conf.setDebug(false);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SJpath", conf, pathTB.createTopology());
		
		Edge e = null;
		while((e = Gstream.nextEdge()) != null)
		{
			ArrayList<FeederSpout> matchSpoutList = Qgraph.getEdgeSpout(e);

			if(matchSpoutList == null) continue;
			for(FeederSpout espout : matchSpoutList)
			{
				espout.feed(new Values(e.s+"", e.t+""));
			}
		}
		
		Utils.sleep(2000);
		cluster.shutdown();
	}
}
