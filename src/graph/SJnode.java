package graph;

import java.util.ArrayList;

public class SJnode {
	
	public SJnode(Edge e){
		ArrayList<Edge> elist = new ArrayList<Edge>();
		elist.add(e);
		this.leafInitial(elist);
	}
	public SJnode(ArrayList<Edge> elist) {
		// TODO Auto-generated constructor stub
		this.leafInitial(elist);
	}
	
	public SJnode(SJnode sjn1, SJnode sjn2)
	{
		childs = new ArrayList<SJnode>();
		vertexes = new ArrayList<Integer>();
		edges = new ArrayList<Edge>();
		childs.add(sjn1);
		childs.add(sjn2);
		for(Edge e : sjn1.getEdges()){
			this.addEdge(e);
			this.addVertex(e.s);
			this.addVertex(e.t);
		}
		for(Edge e : sjn2.getEdges()){
			this.addEdge(e);
			this.addVertex(e.s);
			this.addVertex(e.t);
		}
		/* fields of this node */
		sjFields = new String[vertexes.size()];
		for(int i = 0; i < vertexes.size(); i ++)
		{
			sjFields[i] = vertexes.get(i)+"";
		}
		
		/* join fields of childs */
		String[] fields1 = sjn1.getFields();
		String[] fields2 = sjn2.getFields();
		int i1 = 0;
		int i2 = 0;
		ArrayList<String> tmpSlist = new ArrayList<String>();
		while(i1 < fields1.length && i2 < fields2.length)
		{
			if(fields1[i1].compareTo(fields2[i2]) < 0){
				i1 ++;
				continue;
			}
			if(fields1[i1].compareTo(fields2[i2]) > 0){
				i2 ++;
				continue;
			}
			tmpSlist.add(fields1[i1]);
			i1 ++;
			i2 ++;
		}
		joinFields = new String[tmpSlist.size()];
		tmpSlist.toArray(joinFields);
	}
	
	public boolean isLeaf()
	{
		return childs == null;
	}
	
	public String[] getFields()
	{		
		return this.sjFields;
	}
	
	public String[] getJoinFields()
	{
		return this.joinFields;
	}
	
	public ArrayList<Edge> getEdges()
	{
		return this.edges;
	}

	public String getNodeID(){
		String nid = ""+this.edges.get(0).id;
		for(int i = 1; i < edges.size(); i ++)
		{
			if(edges.get(i-1).id >= edges.get(i).id){
				System.err.println("error edge id");
				System.exit(1);
			}
			nid += "-"+edges.get(i).id;
		}
		return nid;
	}
	
	public ArrayList<SJnode> getChilds(){
		return this.childs;
	}
	
	public String recurString(String _prefix){
		String _str = "";
		_str += this.toString(_prefix);
		if(this.isLeaf()) return _str;
		_str += "\n"+_prefix+"childNum="+childs.size()+"\n";
		for(SJnode child:childs){
			_str+= child.recurString(_prefix+_prefix)+"\n";
		}
		_str+=_prefix;
		for(String j:this.getJoinFields()){
			_str+= j + " ";
		}
		_str+="\n";
		return _str;
	}
	
	public String toString(String _prefix){
		String _str = "";
		_str += _prefix+"nodeID=" + this.getNodeID()+"\t";
		_str += _prefix+"Fields=";
		for(String s : this.getFields()){
			_str += s + " ";
		}
		return _str;		
	}
	
	private boolean addEdge(Edge _e)
	{
		int i = 0;
		for(; i < edges.size(); i ++)
		{
			if(_e.id == edges.get(i).id) return false;
			if(_e.id < edges.get(i).id) break;
		}
		edges.add(i, _e);
		
		return true;
	}
	
	private boolean addVertex(int v)
	{
		int i = 0;
		for(; i < vertexes.size(); i ++)
		{
			if(v == vertexes.get(i)) return false;
			if(v < vertexes.get(i)) break; 
		}
		vertexes.add(i, v);
		return true;
	}
	
	private void leafInitial(ArrayList<Edge> elist)
	{
		// TODO Auto-generated constructor stub
		childs = null;
		vertexes = new ArrayList<Integer>();
		edges = new ArrayList<Edge>();
		for(Edge e : elist){
			this.addEdge(e);
			this.addVertex(e.s);
			this.addVertex(e.t);
		}
		
		sjFields = new String[vertexes.size()];
		for(int i = 0; i < vertexes.size(); i ++)
		{
			sjFields[i] = vertexes.get(i)+"";
		}
		joinFields = null;
	}
	
	
	
	ArrayList<SJnode> childs;
	ArrayList<Integer> vertexes;
	ArrayList<Edge>  edges;
	String[] sjFields;
	String[] joinFields;
}
