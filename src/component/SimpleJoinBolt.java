/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package component;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SimpleJoinBolt extends BaseRichBolt {
	OutputCollector _collector;
	Fields _idFields;
	Fields _outFields;
	int _numSources;
	TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
	Map<String, GlobalStreamId> _fieldLocations;

	public SimpleJoinBolt(Fields outFields) {
		_outFields = outFields;
	}
	
	public ArrayList<String> testList;
	@Override
	public void cleanup() {
		//this.writeList();
	};
	
	public String getStoreFile()
	{
		String fList = "";
		for(String istr: _outFields)
		{
			fList += istr+"_";
		}
		return fList;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) 
	{
		 /* Ecah fieldID(String) comes from which source(GlobalStreamId) */
		_fieldLocations = new HashMap<String, GlobalStreamId>();
		_collector = collector;
		int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
		_pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
		_numSources = context.getThisSources().size();
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) 
		{
			Fields cur_fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(cur_fields.toList());
			if (idFields == null)
				idFields = setFields;
			else/* idFields is the intersection of all visited source's fields */
				idFields.retainAll(setFields);
			
			String myallfields = "";
		
			
			for (String outfield : _outFields) {
				myallfields += outfield+", ";
				for (String sourcefield : cur_fields) {
					if (outfield.equals(sourcefield)) {
						_fieldLocations.put(outfield, source);
					}
				}
			}
			
			System.out.print("myallfields = " + myallfields +"\n");
		}// GlobalStreamID for
		
		String myallidfields = "";
		for(String s : idFields)
		{
			myallidfields+= s+" ";
		}
		
		System.out.print("myallidfield = "+myallidfields+"\n");
		
		_idFields = new Fields(new ArrayList<String>(idFields));
		
		if (_fieldLocations.size() != _outFields.size()) {
			throw new RuntimeException("Cannot find all outfields among sources");
		}
		
		//this.loadList();
	}
	
	@Override
	public void execute(Tuple tuple) 
	{
		List<Object> id = tuple.select(_idFields);
		GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());
		if (!_pending.containsKey(id)) {
			_pending.put(id, new HashMap<GlobalStreamId, Tuple>());
		}
		Map<GlobalStreamId, Tuple> parts = _pending.get(id);
		if (parts.containsKey(streamId))
			throw new RuntimeException("Received same side of single join twice");
		parts.put(streamId, tuple);
		if (parts.size() == _numSources) {
		_pending.remove(id);
		List<Object> joinResult = new ArrayList<Object>();
		for (String outField : _outFields) {
			GlobalStreamId loc = _fieldLocations.get(outField);
			joinResult.add(parts.get(loc).getValueByField(outField));
		}
		_collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);
	
		String myjoinresults = "";
		for(Object jstr : joinResult)
		{
			myjoinresults += jstr.toString() +", ";
		}
		System.out.println("Join: " + myjoinresults);
				
				for (Tuple part : parts.values()) {
					_collector.ack(part);
				}
			}
			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_outFields);
	}

	private class ExpireCallback 
	implements TimeCacheMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> 
	{
		@Override
		public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
			for (Tuple tuple : tuples.values()) {
				_collector.fail(tuple);
			}
		}
	}
}

//public void loadList()
//{
//	String fList = this.getStoreFile();
//	File _file = new File(fList);
//	testList = new ArrayList<String>();
//	if(_file.exists())
//	{
//		BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(_file));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//		e.printStackTrace();
//		}
//	
//		String line = null;
//		while(true)
//		{
//			try {
//				line = br.readLine();
//			} catch (IOException e) {
//			// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			if(line == null) break;
//			testList.add(line);
//		}
//		try {
//			br.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	System.out.print("load " + testList.size()+" from " + fList+"\n");
//}
//public void writeList()
//{
//	String fList = this.getStoreFile();
//	File _file = new File(fList);
//	BufferedWriter bw = null;
//	try {
//		bw = new BufferedWriter(new FileWriter(_file));
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//	for(String s : testList)
//	{
//		try {
//			bw.write(s+"\n");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	try {
//		bw.close();
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//}
//public void addList()
//{
//	testList.add(testList.size()+"->");
//	testList.add(testList.size()+"->");
//	testList.add(testList.size()+"->");
//}
//
