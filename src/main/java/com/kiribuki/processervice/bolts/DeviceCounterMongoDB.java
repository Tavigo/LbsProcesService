package com.kiribuki.processervice.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.json.JSONObject;
import org.json.JSONException;


public class DeviceCounterMongoDB implements IRichBolt {
	

	public static final long serialVersionUID = 1L;
	
	Integer id;
	String name;

	private OutputCollector collector;
	
	//static MongoClient MongoDB;
	static DBCollection table;
	
	static String DBname = "lbs";
	static String CollectionName = "lbsdevices";

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		try {
			MongoClient MongoDB = new MongoClient("54.217.208.12", 27017);
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
			JSONObject json = new JSONObject(str);	
			if (json.has("fromMAC") == false) {
				json.put("fromMAC",  "00:00:00:00:00:00");
			}
			
			BasicDBObject query = new BasicDBObject();
			query.put("deviceID", json.getString("fromMAC"));
	 
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put("Signals", 1);
			
			BasicDBObject updateObj = new BasicDBObject();
			updateObj.put("$inc", newDocument);
			
			table.update(query, updateObj, true, false);
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (JSONException je) {
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
