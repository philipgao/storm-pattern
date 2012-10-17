/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.bolt;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei (eaglelion8038@hotmail.com)
 *
 */
public class ItemCountBolt extends BaseRichBolt {
	private int threshold;
	
	OutputCollector collector;
	Map<String, Set<String>> itemTransactionMap;
	
	/**
	 * @param threshold
	 */
	public ItemCountBolt(int threshold) {
		super();
		this.threshold = threshold;
	}

	/**
	 * @param stormConf
	 * @param context
	 * @param collector
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		itemTransactionMap=new HashMap<String, Set<String>>();
	}

	/**
	 * @param input
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String item = input.getString(0);
		String tid = input.getString(1);
		
		Set<String> transactionSet;
		transactionSet=itemTransactionMap.get(item);
		if(transactionSet==null){
			transactionSet=new HashSet<String>();
			itemTransactionMap.put(item, transactionSet);
		}
		
		transactionSet.add(tid);
		
		System.out.println("*************["+item+" count:"+transactionSet.size()+"]");
		if(transactionSet.size()>=threshold){
			for(String tidForItem:itemTransactionMap.get(item)){
				collector.emit(new Values(tidForItem, item));
			}
		}
		
		collector.ack(input);
	}

	/**
	 * @param declarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tid", "item"));
	}

}
