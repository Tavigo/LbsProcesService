// https://github.com/stormprocessor/storm-sqs/blob/master/src/main/java/storm/contrib/sqs/SqsQueueSpout.java //

package com.kiribuki.processervice.spouts;


import java.util.Map;


//import java.io.IOException;
//import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.amazonaws.AmazonClientException;
//import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
//import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;

//import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import org.json.JSONObject;
import org.json.JSONException;


public class SqsQueueSpout implements IRichSpout {
	
	public static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	private AmazonSQSAsync sqs;

	private final String queueUrl;
	private final boolean reliable;
	private LinkedBlockingQueue<Message> queue;

	private int sleepTime;
	
	/**
	 * @param queueUrl the URL for the Amazon SQS queue to consume from
	 * @param reliable whether this spout uses Storm's reliability facilities
	 */
	public SqsQueueSpout(String queueUrl, boolean reliable) {
		this.queueUrl = queueUrl;
		this.reliable = reliable;
		this.sleepTime = 100;
	}
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		queue = new LinkedBlockingQueue<Message>();
		try {
			sqs = new AmazonSQSAsyncClient(new ClasspathPropertiesFileCredentialsProvider());
			sqs.setRegion(Region.getRegion(Regions.EU_WEST_1));
		} catch (AmazonClientException ace) {
			throw new RuntimeException("Couldn't load AWS credentials", ace);
		}
	}

	public void close() {
		// TODO Auto-generated method stub
		sqs.shutdown();
		// Works around a known bug in the Async clients
		// @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
		((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		if (queue.isEmpty()) {
			// Configuramos la acción ReceiveMessage, para que reciba de 10 en 10 mensajes, y no devuelva
			// el atributo SentTimestamp (tiempo en que el mensaje se envio) de cada uno de los mensajes.
			ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(
					new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(10).withAttributeNames("SentTimestamp"));
		
			queue.addAll(receiveMessageResult.getMessages());
			
		}
		Message message = queue.poll();
		if (message != null) {
			try { 
				JSONObject  json = new JSONObject(message.getBody());
				//Añadimos cuando fue recibido el mensaje en la cola.
				//Utilizamos ISO 8601
				json.put("SentTimestamp",Long.parseLong(message.getAttributes().get("SentTimestamp")));
				if (reliable) {
					collector.emit(getStreamId(message), new Values(json.toString()), message.getReceiptHandle());
				} else {
					// Delete it right away
					sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
					collector.emit(getStreamId(message), new Values(json.toString()), message.getReceiptHandle());
				}
		
			} catch (JSONException e) {
				System.out.println(e.toString());
			}
		} else {
			// Still empty, go to sleep.
			System.out.print("HOLA!!!!");
			Utils.sleep(this.sleepTime);
		}
	
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		// Only called in reliable mode.
		try {
			sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, (String) msgId));
		} catch (AmazonClientException ace) { }
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		// Only called in reliable mode.
		try {
			sqs.changeMessageVisibilityAsync(
					new ChangeMessageVisibilityRequest(queueUrl, (String) msgId, 0));
		} catch (AmazonClientException ace) { }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("message"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}	
	
	
	/**
	 * Returns the stream on which this spout will emit. By default, it is just
	 * {@code Utils.DEFAULT_STREAM_ID}. Simply override this method to send to
	 * a different stream.
	 * 
	 * By using the {@code message} parameter, you can send different messages
	 * to different streams based on context.
	 *
	 * @return the stream on which this spout will emit.
	 */
	public String getStreamId(Message message) {
		return Utils.DEFAULT_STREAM_ID;
	}
	
	/**
	 * Returns the number of milliseconds the spout will wait before making
	 * another call to SQS when the previous call came back empty. Defaults to
	 * {@code 100}.
	 * 
	 * Since Amazon charges per SQS request, you can use this parameter to
	 * control costs for lower-volume queues.
	 * 
	 * @return the number of milliseconds the spout will wait between SQS calls.
	 */
	public int getSleepTime() {
		return sleepTime;
	}
}
