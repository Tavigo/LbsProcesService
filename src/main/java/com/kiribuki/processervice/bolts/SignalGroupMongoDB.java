package com.kiribuki.processervice.bolts;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Date;
import java.sql.Timestamp;

import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SignalGroupMongoDB implements IRichBolt {
	public static final long serialVersionUID = 1L;
	
	Integer id;
	String name;
	private OutputCollector collector;
	
	//static MongoClient MongoDB;
	static DBCollection RSSIavg;
	static DBCollection nodegroup;
	
	static String DBname = "lbs";

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
			RSSIavg= db.getCollection("lbsRSSIavg");
			nodegroup = db.getCollection("lbsnodegroup");
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
			// Consultamos si hay más nodeCapture que han detectado la señal de la
			// estación.
			
			// Buscamos si existe ya el registro en la colección
			BasicDBObject query;
			BasicDBObject update;
			boolean upsert;
			query = new BasicDBObject("fromMAC",json.getString("fromMAC"))
				.append("nodeMACS.nodeMAC",json.getString("nodeMAC"));  
			
			
			DBObject xiu = nodegroup.findOne(query);
			System.out.println(xiu);
			
			// Miramos si hay un registro en la colección con estos valores
			if (xiu == null){
				query = new BasicDBObject("fromMAC",json.getString("fromMAC"));
				BasicDBObject fields = new BasicDBObject("nodeMAC", json.getString("nodeMAC"))
				                 .append("RSSIavg", json.getInt("RSSIavg"))
				                 .append("SentTimestamp", json.getLong("SentTimestamp"))
				                 .append("x", json.getLong("x"))
				                 .append("y", json.getLong("y"));
				update = new BasicDBObject("$push", new BasicDBObject("nodeMACS", fields));
				upsert = true;				
			} else {
				update = new BasicDBObject("$set", new BasicDBObject("nodeMACS.$.RSSIavg", json.getInt("RSSIavg"))
				                    .append("nodeMACS.$.SentTimestamp", json.getLong("SentTimestamp"))
				                    .append("nodeMACS.$.x", json.getLong("x"))
				                    .append("nodeMACS.$.y", json.getLong("y")));
				upsert = false;	
			}
			
			//System.out.println(query);
			//System.out.println(update);
			
			nodegroup.update(query, update, upsert, false);
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
