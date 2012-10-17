/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei (eaglelion8038@hotmail.com)
 *
 */
public class FileTransactionItemSpout extends BaseRichSpout {
	private String filename;
	
	private SpoutOutputCollector collector;
	private BufferedReader reader;
	
	
	/**
	 * @param filename
	 */
	public FileTransactionItemSpout(String filename) {
		super();
		this.filename = filename;
	}

	/**
	 * @param conf
	 * @param context
	 * @param collector
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		
		try {
			reader=new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		if(reader!=null){
			try {
				String line= reader.readLine();
				
				if(line!=null && line.length()>0 && !line.startsWith("#")){
					String [] values=line.split(" ");
					String tid=values[0];
					String item=values[1];
					
					collector.emit(new Values(item, tid));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}

	/**
	 * @param declarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("item","tid"));
	}

}
