/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.topo;

import java.util.StringTokenizer;

import com.ssparrow.storm.pattern.bolt.ItemCountBolt;
import com.ssparrow.storm.pattern.bolt.ItemSetCountBolt;
import com.ssparrow.storm.pattern.bolt.PatternOutputBolt;
import com.ssparrow.storm.pattern.bolt.TransactionItemRebuildBolt;
import com.ssparrow.storm.pattern.spout.RandomTransactionItemBolt;
import com.ssparrow.storm.pattern.spout.FileTransactionItemSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Gao, Fei (eaglelion8038@hotmail.com)
 *
 */
public class AssociatePatternMiningTopology {

	
	public static void main(String [] args) throws Exception{
		String filename="test/data/transaction.txt";
		int threshold=3;
		InputType inputType=InputType.FILE;
		ClusterType clusterType=ClusterType.LOCAL;
		String clusterName="";
		int timeout=30;
		
		if(args.length>0){
			StringTokenizer st=new StringTokenizer(args[0]);
			while(st.hasMoreTokens()){
				String arg = st.nextToken();
				
				System.out.println("!!!!"+arg);
				if(arg.equals("-help")||arg.equals("-h")){
					System.out.println("Usage:");
					System.out.println("\t run.sh [-i FILE] [-f test/data/transaction.txt] [-t 3] [-ct LOCAL] [-to 15]");
					System.out.println("\t run.sh [-i RANDOM] [-t 10] [-ct LOCAL] [-to 0]");
					return;
				}else if(arg.equals("-inputtype")||arg.equals("-i")){
					if(st.hasMoreTokens()){
						inputType=InputType.valueOf(st.nextToken());
					}
				}else if(arg.equals("-file")||arg.equals("-f")){
					if(st.hasMoreTokens()){
						filename=st.nextToken();
					}
				}else if(arg.equals("-threshold")||arg.equals("-t")){
					if(st.hasMoreTokens()){
						threshold=Integer.parseInt(st.nextToken());
					}
				}else if(arg.equals("-clustertype")||arg.equals("-ct")){
					if(st.hasMoreTokens()){
						clusterType=ClusterType.valueOf(st.nextToken());
					}
				}else if(arg.equals("-clustername")||arg.equals("-cn")){
					if(st.hasMoreTokens()){
						clusterType=ClusterType.valueOf(st.nextToken());
					}
				}else if(arg.equals("-timeout")||arg.equals("-to")){
					if(st.hasMoreTokens()){
						timeout=Integer.parseInt(st.nextToken());
					}
				}
			}
		}
		
		TopologyBuilder builder=new TopologyBuilder();
		
		if(inputType==InputType.FILE){
			builder.setSpout("TransactionItemSpout", new FileTransactionItemSpout(filename));
		}else{
			builder.setSpout("TransactionItemSpout", new RandomTransactionItemBolt());
		}
		
		builder.setBolt("SingleItemCount", new ItemCountBolt(threshold), 5).fieldsGrouping("TransactionItemSpout", new Fields("item"));
		builder.setBolt("TransactionItemRebuild", new TransactionItemRebuildBolt(), 5).fieldsGrouping("SingleItemCount", new Fields("tid"));
		builder.setBolt("ItemSetCount", new ItemSetCountBolt(threshold), 5).fieldsGrouping("TransactionItemRebuild", new Fields("itemset"));
		builder.setBolt("PatternOutput", new PatternOutputBolt(), 5).fieldsGrouping("ItemSetCount", new Fields("itemsetstr"));
		
		Config conf=new Config();
		conf.setDebug(true);
		
		if(clusterType==ClusterType.REMOTE){
			conf.setNumWorkers(3);
			
			StormSubmitter.submitTopology(clusterName, conf, builder.createTopology());
		}else{
			conf.setMaxTaskParallelism(5);
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("word-count", conf, builder.createTopology());
			
			if(timeout>0){
				Thread.sleep(timeout*1000);
			
				localCluster.shutdown();
			}
		}
	}
	
	enum InputType{
		FILE, RANDOM;
	}
	
	enum ClusterType{
		LOCAL, REMOTE;
	}
}
