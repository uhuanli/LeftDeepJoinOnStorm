package graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import component.SimpleJoinBolt;

public class QueryGraph {

	public QueryGraph(String _qpath){
		
		try {
			loadEdges(_qpath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		buildAdjacents();
		edgespouts = null;
		sjRoot = null;
	}
	
	public void loadEdges(String _qpath) throws IOException
	{
		BufferedReader _br = null;
		_br = new BufferedReader(new FileReader(_qpath));
		String _line = null;
		Enum  = 0;
		allEdges = new ArrayList<Edge>();
		while((_line = _br.readLine()) != null)
		{
			if(_line.length() < 2) continue;
			Edge e = new Edge(_line, Enum);
			allEdges.add(e);
			Enum ++;
		}
		_br.close();
	}
	
	public void buildAdjacents()
	{
		HashSet<Integer> vset = new HashSet<Integer>();
		for(Edge e : allEdges)
		{
			vset.add(e.s);
			vset.add(e.t);
		}
		
		Vnum = vset.size();
	}
	
	public FeederSpout[] getEdgeSpouts()
	{
		return this.edgespouts;
	}
	
	public ArrayList<FeederSpout> getEdgeSpout(Edge _e)
	{
		ArrayList<FeederSpout> espoutList = null;
		for(Edge e : allEdges)
		{
			if(! e.match(_e)) continue;
			if(espoutList == null){
				espoutList = new ArrayList<FeederSpout>();
			}
			espoutList.add(this.edgespouts[e.id]);			
		}
		
		return espoutList;
	}
	
	private void buildSJtree()
	{
		
		return;
	}
	
	public void buildLeftDeepSJtree()
	{
		sjRoot = new SJnode(this.allEdges.get(0));
		for(int i = 1; i < Enum; i ++)
		{
			sjRoot = new SJnode( sjRoot, new SJnode(this.allEdges.get(i)) );
		}
//		System.out.println(sjRoot.recurString(" "));
		return;
	}
	
	public TopologyBuilder getLeftDeepTopology2()
	{
		if(Enum <= 0 || Enum != allEdges.size()){
			System.err.println("edge err");
			System.exit(-1);
		}		
		edgespouts = new FeederSpout[Enum];
		
		this.buildLeftDeepSJtree();
		TopologyBuilder _tb = this.BuilderOverSJtree(sjRoot);
		
		return _tb;
	}
	
	public TopologyBuilder getLeftDeepTopology()
	{
		if(Enum <= 0 || Enum != allEdges.size()){
			System.err.println("edge err");
			System.exit(-1);
		}
		TopologyBuilder _tb = new TopologyBuilder();
		
		edgespouts = new FeederSpout[Enum];
		{// spouts 
			for(int i = 0; i < Enum; i ++)
			{
				Edge e = allEdges.get(i);
				edgespouts[i] = new FeederSpout(new Fields(e.s+"", e.t+""));
				_tb.setSpout(e.id+"", edgespouts[i]);
			}
		}
		{// bolts
			ArrayList<String> vertex_field = new ArrayList<String>();
			vertex_field.add(allEdges.get(0).s+"");
			for(int i = 1; i < Enum; i ++)
			vertex_field.add(allEdges.get(0).t+"");
			String preBoltID = allEdges.get(0).id+"";
			for(int i = 1; i < Enum; i ++)
			{
				Edge e = allEdges.get(i);
				String newid = preBoltID + "-" + e.id;
				vertex_field.add(e.t+"");
				_tb.setBolt(newid, new SimpleJoinBolt(new Fields(vertex_field)))
									 .fieldsGrouping(preBoltID, new Fields(e.s+""))
									 .fieldsGrouping(e.id+"", new Fields(e.s+""));
			}
		}
		
		return _tb;
	}
	
	public TopologyBuilder getGraphTopology()
	{
		TopologyBuilder _tb = new TopologyBuilder();
		{// spouts
			FeederSpout[] edgespouts = new FeederSpout[Enum]; 
			for(int i = 0; i < Enum; i ++)
			{
				Edge e = allEdges.get(i);
				edgespouts[i] = new FeederSpout(new Fields(e.id+""));
				_tb.setSpout(e.id+"", edgespouts[i]);
			}
		}
		{// bolts
			
		}
		
		return _tb;
	}
	
	private int curSpout; 
	private TopologyBuilder BuilderOverSJtree(SJnode sjroot){
		TopologyBuilder _tb = new TopologyBuilder();
		curSpout = 0;
		SJnodeComponent(sjroot, _tb);
		if(curSpout != Enum) {
			System.out.println("error curspoutNum");
			System.exit(-1);
		}
		return _tb;
	}
	
	private void SJnodeComponent(SJnode curnode, TopologyBuilder _tb){
		if(curnode.isLeaf()){
			edgespouts[curSpout] = new FeederSpout(new Fields(curnode.getFields()));
			_tb.setSpout(curnode.getNodeID(), edgespouts[curSpout]);
			curSpout ++;
			//System.out.println("leaf=" + curnode.toString(""));
			return;
		}
		
		for(SJnode child : curnode.getChilds()){
			this.SJnodeComponent(child, _tb);
		}
		
		BoltDeclarer bd = 
			_tb.setBolt(curnode.getNodeID(), new SimpleJoinBolt(new Fields(curnode.getFields())));
		
		for(SJnode child : curnode.getChilds()){
			bd.fieldsGrouping(child.getNodeID(), new Fields(curnode.getJoinFields()));
		}
		
		return;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

	ArrayList<Edge> allEdges;
	FeederSpout[] edgespouts;
	SJnode sjRoot;
	
	int Vnum;
	int Enum;
}