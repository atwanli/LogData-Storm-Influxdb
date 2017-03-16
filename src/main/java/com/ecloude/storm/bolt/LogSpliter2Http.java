package com.ecloude.storm.bolt;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * @Title: LogSpliter.java
 * @Package cn.itcast.storm.bolt
 * @Description: TODO(分割日志,拼接数据)
 * @author wwl
 * @date 2016年7月18日 下午6:06:23
 * @version V1.0
 */
public class LogSpliter2Http extends BaseBasicBolt {
	
	private String host;
	private static final long serialVersionUID = -5653803832498574866L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		host = (String) stormConf.get("HOST");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] datas = line.split("\\|");
		if(datas.length<10)return;
		if("0".equals(datas[3])){
			pay(datas, collector);
		}else if("1".equals(datas[3])){
			refund(datas, collector);
		}else if("2".equals(datas[3])){
			query(datas, collector);
		}else if("3".equals(datas[3])){
			sign(datas, collector);
		}
	
	}

	/**
	 * 支付
	 * 
	 * @param datas
	 * @param collector
	 */
	private void pay(String[] datas, BasicOutputCollector collector) {
		if ("null".equals(datas[6]))
			datas[6] = "0";
		if ("null".equals(datas[7]))
			datas[7] = "0";
		String jsonData="[{\"name\": \"pay\",\"columns\": [\"time\", \"host\", \"merId\", \"orderFlag\", \"allTime\", \"ecifTime\", \"icqTime\"],\"points\": [[%s, \"%s\",\"%s\", \"%s\", %s, %s, %s]]}]";
		String urlBody=String.format(jsonData, Long.valueOf(datas[9])+Long.valueOf(host),datas[1], datas[2], datas[4], datas[5], datas[6], datas[7]);
		collector.emit(new Values(urlBody));

	}

	/**
	 * 退货
	 * 
	 * @param datas
	 * @param collector
	 */
	private void refund(String[] datas, BasicOutputCollector collector) {
		if ("null".equals(datas[7]))
			datas[7] = "0";
		String dbData = "%s,host=%s,merId=%s,orderFlag=%s allTime=%s,icqTime=%s %s'";
		String urlBody = String.format(dbData, "refund", datas[1], datas[2], datas[4], datas[5], datas[7], Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlBody));

	}

	/**
	 * 查证
	 * 
	 * @param datas
	 * @param collector
	 */
	private void query(String[] datas, BasicOutputCollector collector) {
		if ("null".equals(datas[7]))
			datas[7] = "0";
		String dbData = "%s,host=%s,merId=%s,orderFlag=%s allTime=%s,icqTime=%s %s'";
		String urlBody = String.format(dbData, "query", datas[1], datas[2], datas[4], datas[5], datas[7], Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlBody));
	}

	/**
	 * 签约
	 * 
	 * @param datas
	 * @param collector
	 */
	private void sign(String[] datas, BasicOutputCollector collector) {
		if ("null".equals(datas[6]))
			datas[6] = "0";
		if ("null".equals(datas[7]))
			datas[7] = "0";
		if ("null".equals(datas[8]))
			datas[8] = "0";
		String jsonData="["+
		  "{"+
		   "\"name\": \"sign\","+
		   "\"columns\": [\"time\", \"host\", \"merId\", \"orderFlag\", \"allTime\", \"ecifTime\", \"icqTime\"],"+
		    "\"points\": ["
		    + " [%s, %s, %s, %s, %s, %s, %s, %s]"
		   +" ]"
		  +"}"
		+"]";
		String urlBody=String.format(jsonData, Long.valueOf(datas[9])+Long.valueOf(host),datas[1], datas[2], datas[4], datas[5], datas[6], datas[7]);
		collector.emit(new Values(urlBody));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

}
