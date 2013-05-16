package com.kiribuki.processervice.bolts;

import java.util.HashMap;
import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
//import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
//import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
//import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
//import com.amazonaws.services.sqs.AmazonSQSAsync;
//import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
//import com.amazonaws.services.sqs.AmazonSQSClient;
//import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
//import com.amazonaws.services.sqs.model.DeleteMessageRequest;
//import com.amazonaws.services.sqs.model.Message;
//import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
//import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import com.google.gson.Gson;
import com.kiribuki.processervice.PacketCapture;

public class DeviceCounter implements IRichBolt {
	
	Integer id;
	String name;

	private OutputCollector collector;
	
	static AmazonDynamoDBClient dynamoDB;
	static String tableName = "lbs-devices";

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		
		try {
		    dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
		    dynamoDB.setRegion(Region.getRegion(Regions.EU_WEST_1));
		} catch (AmazonClientException ace) {
			throw new RuntimeException("No se pueden cargar las credenciales AWS", ace);
		}
		
	}	
	
	
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str = input.getString(0);
		PacketCapture packet = new PacketCapture();
		Gson gson = new Gson();
		packet = gson.fromJson(str, PacketCapture.class);
		
		if ( packet.GetfromMAC() == null ) {
			packet.SetfromMAC("00:00:00:00:00:00");
		}
		try {
			System.out.printf("fromMAC: %s\n", packet.GetfromMAC());
			System.out.printf("COMPONENT: %s ID: %d\n", this.name, this.id);
				
			Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put("deviceID", new AttributeValue(packet.GetfromMAC()));

			updateItems.put("signal", 
				 new AttributeValueUpdate()
				   .withAction(AttributeAction.ADD)
				   .withValue(new AttributeValue().withN("+1")));
				           
			UpdateItemRequest updateItemRequest = new UpdateItemRequest()
				 .withTableName(tableName)
				 .withKey(key).withReturnValues(ReturnValue.UPDATED_NEW)
				 .withAttributeUpdates(updateItems);
				            
			UpdateItemResult result = dynamoDB.updateItem(updateItemRequest);
			System.out.println("Result: " + result);
	      } catch (AmazonServiceException ase) {
	           System.out.println("Caught an AmazonServiceException, which means your request made it "
	                    + "to AWS, but was rejected with an error response for some reason.");
	           System.out.println("Error Message:    " + ase.getMessage());
	           System.out.println("HTTP Status Code: " + ase.getStatusCode());
	           System.out.println("AWS Error Code:   " + ase.getErrorCode());
	           System.out.println("Error Type:       " + ase.getErrorType());
	           System.out.println("Request ID:       " + ase.getRequestId());
	       } catch (AmazonClientException ace) {
	           System.out.println("Caught an AmazonClientException, which means the client encountered "
	                    + "a serious internal problem while trying to communicate with AWS, "
	                    + "such as not being able to access the network.");
	           System.out.println("Error Message: " + ace.getMessage());
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
