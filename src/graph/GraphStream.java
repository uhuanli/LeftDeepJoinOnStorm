package graph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class GraphStream {
	public GraphStream(String _datapath){
		dat_path = _datapath;
		initial();
	}
	
	public void initial()
	{
		try {
			br = new BufferedReader(new FileReader(dat_path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		timestamp = 0;
	}
	
	public Edge nextEdge()
	{
		Edge e = null;
		String line = null;
		try {
			line = this.nextLine();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if(line != null)
		{
			e = new Edge(line, timestamp);
			timestamp ++;
		}else{
			System.out.println("Stream is empty now");
		}
		
		return e;
	}
	
	private String nextLine() throws IOException {
		String line = br.readLine();
		if(line == null)
		{
			br.close();
		}
		return line;
	}
	
	int timestamp;
	BufferedReader br;
	String dat_path;
}
