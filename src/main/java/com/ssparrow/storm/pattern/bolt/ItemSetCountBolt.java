/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
public class ItemSetCountBolt  extends BaseRichBolt {
	private int threshold;
	private OutputCollector collector;
	
	private Map<Set<String>, Set<String>> itemSetTransactionSetMap;

	/**
	 * @param threshold
	 */
	public ItemSetCountBolt(int threshold) {
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
		itemSetTransactionSetMap=new HashMap<Set<String>, Set<String>>();
	}

	/**
	 * @param input
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		Set<String> itemSet = (Set<String>) input.getValue(0);
		String tid = input.getString(1);
		
		Set<String> transactionSet = itemSetTransactionSetMap.get(itemSet);
		if(transactionSet==null){
			transactionSet=new HashSet<String>();
			itemSetTransactionSetMap.put(itemSet, transactionSet);
		}
		
		transactionSet.add(tid);
		
		if(transactionSet.size()>=threshold){
			collector.emit(new Values(itemSet.toString(), transactionSet.size()));
		}
		
		collector.ack(input);
	}

	/**
	 * @param declarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itemsetstr","count"));
	}

}
