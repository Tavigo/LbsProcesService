package com.kiribuki.processervice;

import com.kiribuki.processervice.spouts.SqsQueueSpout;
import com.kiribuki.processervice.bolts.DeviceCounter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;



public class TopologyMain {

	static String queueURL;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		if (args.length >= 1) {
			queueURL = args[0];
	
		} else {
			System.out.println("Falta parámetro de entrada; cola ha consumirS");
			System.exit(1);  
		}
		
		// Definición de la topologia de STORM
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sqs-queue-reader",  new SqsQueueSpout(queueURL,false));
		builder.setBolt("device-counter",new DeviceCounter(),5).shuffleGrouping("sqs-queue-reader");
		
		//Configuración 
		Config conf = new Config();
		conf.setDebug(false);
		
		//Correr la topologia.
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		//conf.put(Config.TOPOLOGY_DEBUG, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
		Thread.sleep(200000);
		cluster.shutdown();
	}

}
