package com.kiribuki.processervice.bolts;

import java.net.UnknownHostException;
import java.util.Map;

import com.mongodb.DBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SignalHistoryMongoDB implements IRichBolt {
	public static final long serialVersionUID = 1L;
	
	Integer id;
	String name;

	private OutputCollector collector;
	
	//static MongoClient MongoDB;
	static DBCollection table;
	
	static String DBname = "lbs";
	static String CollectionName = "lbshistory";

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.name = context.getThisComponentId();		
		this.id = context.getThisTaskId();
		try {
			MongoClient MongoDB = 
					new MongoClient(stormConf.get("mongodbHOST").toString(), 27017);
			DB db = MongoDB.getDB(DBname);
			table = db.getCollection(CollectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str = input.getString(0);
	
		try {	
			table.insert((DBObject)JSON.parse(str));
		} catch (MongoException e) {
			e.printStackTrace();
		}catch (Exception je) {
			je.printStackTrace();
		}
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
