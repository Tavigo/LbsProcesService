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
import com.mongodb.DBCursor;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.json.JSONObject;
import org.json.JSONException;

public class SignalCounterMongoDB implements IRichBolt {
	public static final long serialVersionUID = 1L;
	
	Integer id;
	String name;

	private OutputCollector collector;
	
	//static MongoClient MongoDB;
	static DBCollection table;
	
	static String DBname = "lbs";
	static String CollectionName = "lbssignals";
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
		DBCursor cursor;
		JSONObject json;
		BasicDBObject query;
		double senttime=0;
	
		try {
			json = new JSONObject(str);	
			if (json.has("fromMAC") == false) {
				json.put("fromMAC",  "00:00:00:00:00:00");
			}
			//Objeto Query a la base de datos, para consultar si hay más señales de este tipo
			query = new BasicDBObject();
			query.put("KeySignalHash", json.getString("SignalHash"));
			query.put("KeyfromMAC", json.getString("fromMAC"));
			query.put("KeyTimestamp", new BasicDBObject("$lte", json.getDouble("SentTimestamp") + 200)
			                .append("$gte", json.getDouble("SentTimestamp") - 200));
			// Cerquem el registre, si existeix a la base de dades
			
			System.out.println(query.toString());
		
			
			cursor = table.find(query);

			if (cursor.count() == 0 ) {
				senttime = json.getDouble("SentTimestamp");
			} else {
				try {
					while(cursor.hasNext()) {
						senttime = Double.parseDouble(cursor.next().get("KeyTimestamp").toString());
					}
				} finally {
					  cursor.close();
				}
			}
			query = new BasicDBObject();
			query.put("KeySignalHash", json.getString("SignalHash"));
			query.put("KeyfromMAC", json.getString("fromMAC"));
			query.put("KeyTimestamp", senttime );
			
			//Incrementamos uno el contador de señales del mismo tipo
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put("Signals", 1);
			BasicDBObject updateObj = new BasicDBObject();
			updateObj.put("$inc", newDocument);
			table.update(query, updateObj, true, false);
			
			//Añadimos los datos de la señal recibida
			BasicDBObject dbl = new BasicDBObject();
			
			dbl.put("nodeMAC", json.getString("nodeMAC"));
			dbl.put("latitud", json.getDouble("latitud"));
			dbl.put("longitud", json.getDouble("longitud"));
			dbl.put("tipopaquete", json.getString("tipopaquete"));
			dbl.put("RSSI", json.getInt("RSSI"));
			dbl.put("FechaCaptura",json.getDouble("FechaCaptura"));
			dbl.put("SentTimestamp",json.getDouble("SentTimestamp"));
		
			// Nuevo documento
			BasicDBObject newArray = new BasicDBObject();
			newArray.put("datos", dbl);
			
			BasicDBObject updateArray = new BasicDBObject();
			updateArray.put("$push", newArray);
			table.update(query, updateArray, true, false);
					
		} catch (MongoException e) {
			e.printStackTrace();
		}catch (JSONException je) {
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
