/**
 * 
 */
package com.ssparrow.storm.pattern.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class RandomTransactionItemBolt extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String [] items;
	
	private Random randomItem;
	private Random randomTransaction;

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		this.collector=collector;
		
		items=new String[26];
		for(int i=0;i<26;i++){
			char currrentChar=(char) ('a'+i);
			items[i]=new String(new char[]{currrentChar});
		}
		
		randomItem=new Random();
		randomTransaction=new Random();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		int itemIndex=randomItem.nextInt(items.length);
		String item=items[itemIndex];
		
		String tid=String.valueOf(randomTransaction.nextInt(100)+1);
		
		collector.emit(new Values(item, tid));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("item","tid"));
	}

}
