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
public class LogSpliter extends BaseBasicBolt {
	
	private String host;
	private String INFLUXDB_URL;
	private static final long serialVersionUID = -5653803832498574866L;
//	private static final String[] order = { "pay", "refund", "query", "sign" };

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		host = (String) stormConf.get("HOST");
		INFLUXDB_URL = (String) stormConf.get("INFLUXDB_URL");
//		INFLUXDB_URL=PropertyUtil.getProperty("influxdb_url");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// |host|merId|orderType|orderFlag|allTime|ecifTime|icqTime|icqTime|timeFlag
		// sum|195.203.56.35|305110099990002|0|0|2067|3|554|null|1472803352045000000|END
		String line = input.getString(0);
		String[] datas = line.split("\\|");
		// String host=datas[1];
		// String merId=datas[2];
		// String orderType=datas[3];
		// String orderFlag=datas[4];
		// String allTime=datas[5];
		// String ecifTime=datas[6];
		// String icqTime=datas[7];
		// String icq1Time=datas[8];
		// String timeStamp=datas[9];
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
		String dbData = "curl -i -X POST '%s/write?db=online' --data-binary '%s,host=%s,merId=%s,orderFlag=%s allTime=%s,ecifTime=%s,icqTime=%s %s'";
		String urlData = String.format(dbData, INFLUXDB_URL,"pay", datas[1], datas[2], datas[4], datas[5], datas[6], datas[7],
				Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlData));

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
		String dbData = "curl -i -X POST '%s/write?db=online' --data-binary '%s,host=%s,merId=%s,orderFlag=%s allTime=%s,icqTime=%s %s'";
		String urlData = String.format(dbData,INFLUXDB_URL, "refund", datas[1], datas[2], datas[4], datas[5], datas[7], Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlData));

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
		String dbData = "curl -i -X POST '%s/write?db=online' --data-binary '%s,host=%s,merId=%s,orderFlag=%s allTime=%s,icqTime=%s %s'";
		String urlData = String.format(dbData,INFLUXDB_URL, "query", datas[1], datas[2], datas[4], datas[5], datas[7], Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlData));
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
		String dbData = "curl -i -X POST '%s/write?db=online' --data-binary '%s,host=%s,merId=%s,orderFlag=%s allTime=%s,ecifTime=%s,icqTime=%s,icqTime1=%s %s'";
		String urlData = String.format(dbData, INFLUXDB_URL, "sign", datas[1], datas[2], datas[4], datas[5], datas[6], datas[7],
				datas[8], Long.valueOf(datas[9])+Long.valueOf(host));
		collector.emit(new Values(urlData));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

}
