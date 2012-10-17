/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @author Gao, Fei (eaglelion8038@hotmail.com)
 *
 */
public class PatternOutputBolt extends BaseRichBolt {
	private OutputCollector collector;

	/**
	 * @param stormConf
	 * @param context
	 * @param collector
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	/**
	 * @param input
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String itemSetStr = input.getString(0);
		int count = input.getInteger(1);
		
		System.out.println("!!!!!Pattern Found, Item Set:"+itemSetStr+", Count:"+count);
		
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
