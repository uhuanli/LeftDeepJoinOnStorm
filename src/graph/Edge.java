package graph;

public class Edge implements Comparable<Edge>
{
	public  Edge(int _s, int _t, String _label, int _id) {
		s = _s;
		t = _t;
		label = _label;
		id = _id;
	}
	
	public Edge(String line, int _id)
	{
		String[] sp = line.split(" ");
		s = Integer.parseInt(sp[0]);
		t = Integer.parseInt(sp[1]);
		label  = sp[2];
		if(sp.length > 3)
		{
			System.err.println("sp len > 3");
			System.exit(-1);
		}
		id = _id;
	}
	
	public boolean match(Edge e)
	{
		return this.label.equals(e.label);
	}
	
	public boolean sameEdge(Edge e)
	{
		if(this.s != e.s) return false;
		
		return this.t == e.t;
	}
	public boolean LT(Edge e)
	{
		if(this.s < e.s) return true;
		if(this.s == e.s) return this.t < e.t;
		
		return false;
	}
	
	public int getTimeStamp()
	{
		return id;
	}
	
	@Override
	public int compareTo(Edge eo) {
		// TODO Auto-generated method stub
		if(this.s != eo.s) return this.s - eo.s;
		
		return this.t - eo.t;
	}
	
	int id;
	public int s;
	public int t;
	public String label;

}
