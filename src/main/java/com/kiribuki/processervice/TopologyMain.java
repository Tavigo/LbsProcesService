package com.kiribuki.processervice;

import com.kiribuki.processervice.spouts.SqsQueueSpout;

import com.kiribuki.processervice.bolts.DeviceCounterMongoDB;
import com.kiribuki.processervice.bolts.SignalCounterMongoDB;
import com.kiribuki.processervice.bolts.SignalHistoryMongoDB; 

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;



public class TopologyMain {

	static String queueURL=null;
	static String mongodbHOST=null;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		if (args.length >= 1) {
			queueURL = args[0];
			mongodbHOST = args[1];
		} else {
			System.out.println("Falta parámetro de entrada; cola ha consumir");
			System.exit(1);  
		}
		
		// Definición de la topologia de STORM
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sqs-queue-reader",  new SqsQueueSpout(queueURL,false));
		//builder.setBolt("device-counter-MongoDB",new DeviceCounterMongoDB(),5).shuffleGrouping("sqs-queue-reader");
		//builder.setBolt("signal-counter-MongoDB",new SignalCounterMongoDB(),5).shuffleGrouping("sqs-queue-reader");
		builder.setBolt("history-signal-MongoDB",new SignalHistoryMongoDB(),20).shuffleGrouping("sqs-queue-reader");
		//Configuración: Aquí podemos poner los parámetros de configuración que 
		// queremos que sean visibles en toda la topologia
		Config conf = new Config();
		conf.setDebug(false);
		conf.put("mongodbHOST", mongodbHOST);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		//conf.put(Config.TOPOLOGY_DEBUG, 1);
		
		// Ponemos en marcha la topologia
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
		Thread.sleep(20000000);
		cluster.shutdown();
	}

}
