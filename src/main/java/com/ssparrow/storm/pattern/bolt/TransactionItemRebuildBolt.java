/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
public class TransactionItemRebuildBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, Set<String>> transactionItemMap;
	
	/**
	 * @param stormConf
	 * @param context
	 * @param collector
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		transactionItemMap=new HashMap<String, Set<String>>();
	}

	/**
	 * @param input
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String tid = input.getString(0);
		String item = input.getString(1);
		
		Set<String> itemSet = transactionItemMap.get(tid);
		
		if(itemSet==null){
			itemSet=new HashSet<String>();;
			transactionItemMap.put(tid, itemSet);
			
			itemSet.add(item);
		}else if(!itemSet.contains(item)){
			if(itemSet.size()>=1){
				List<Set<String>> allSubSets = getAllSubSets(itemSet);
				
				for(Set<String> subSet:allSubSets){
					subSet.add(item);
					
					collector.emit(new Values(subSet, tid));
				}
			}
			
			itemSet.add(item);
		}
		
		collector.ack(input);
	}
	
	private List<Set<String>> getAllSubSets(Set<String> itemSet){
		List<String> itemList = new ArrayList<String>(itemSet);
		
		List<Set<String>> allSubSets=new ArrayList<Set<String>>();

		getAllSubSets(allSubSets, itemList, 0);
		
		allSubSets.remove(new TreeSet<String>());
		
		return allSubSets;
	}
	
	private void getAllSubSets(List<Set<String>> allSubSets,List<String> itemList, int position){
		if(position==itemList.size()){
			allSubSets.add(new TreeSet<String>());
			return;
		}
		
		getAllSubSets(allSubSets, itemList, position+1);
		
		String currentItem=itemList.get(position);
		List<Set<String>> newSubSetList =new ArrayList<Set<String>>();
		for(Set<String> subSet:allSubSets){
			Set<String> newSubSet=new TreeSet<String>(subSet);
			newSubSet.add(currentItem);
			newSubSetList.add(newSubSet);
		}
		
		allSubSets.addAll(newSubSetList);
	}
	
	

	/**
	 * @param declarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itemset", "tid"));
	}

}
