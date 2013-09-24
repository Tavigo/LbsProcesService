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

public class SignalRSSIAvgMongoDB implements IRichBolt {
	public static final long serialVersionUID = 1L;
	
	Integer id;
	String name;
	private OutputCollector collector;
	
	//static MongoClient MongoDB;
	static DBCollection table;
	static DBCollection history;
	
	static String DBname = "lbs";
	static String CollectionName = "lbsRSSIavg";

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
			history = db.getCollection("lbshistory");
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
			int RSSIavg=0;
			JSONObject json = new JSONObject(str);
			// Restamos un minuto al tiempo en que el mensaje llega a la cola. 
			// Trabajaremos con ventanas de 1 minuto para calcular la media del RSSI
			long ventana = json.getLong("SentTimestamp") - 1*60*1000;			
			
			// Calculamos la media del RSSI para el dispositivo seleccionado.
			BasicDBObject matchfields = new BasicDBObject("fromMAC",json.getString("fromMAC"))
				.append("nodeMAC", json.getString("nodeMAC"))
				.append("SentTimestamp", new BasicDBObject("$gt",ventana));  
			BasicDBObject match = new BasicDBObject("$match", matchfields);		
			BasicDBObject groupfields = new BasicDBObject("_id", "$fromMAC")
				.append("avg", new BasicDBObject("$avg", "$RSSI"));
			BasicDBObject group = new BasicDBObject("$group", groupfields);
						
			AggregationOutput output = history.aggregate( match, group );
			for (DBObject obj : output.results()) {
			    RSSIavg = Math.round(Float.parseFloat(obj.get("avg").toString()));
			}
			
			if (RSSIavg == 0) {
				RSSIavg = json.getInt("RSSI");
			}
			// Instertamos el nuevo registro en la colección 	
			BasicDBObject doc = new BasicDBObject("fromMAC", json.getString("fromMAC"))
			     .append("nodeMAC", json.getString("nodeMAC"))
			     .append("RSSI", json.getInt("RSSI"))
			     .append("RSSIavg", RSSIavg)
			     .append("SentTimestamp", json.getLong("SentTimestamp"))
			     .append("x",json.getLong("longitud"))
			     .append("y", json.getLong("latitud"));			
			table.insert(doc);
			collector.emit(Utils.DEFAULT_STREAM_ID, new Values(doc.toString()));
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
		declarer.declare(new Fields("RSSIavg"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
