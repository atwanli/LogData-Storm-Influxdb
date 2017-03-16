package com.ecloude.storm.bolt;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

/**
 * @Title: LogSpliter.java
 * @Package cn.itcast.storm.bolt
 * @Description: TODO(分割日志,拼接数据)
 * @author wwl
 * @date 2016年7月18日 下午6:06:23
 * @version V1.0
 */
public class LogSpliter3Database extends BaseBasicBolt {

	private String host;
	private static final long serialVersionUID = -5653803832498574866L;
	private InfluxDB influxDB;
	private static final Logger logger = Logger.getLogger("order");
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		host = (String) stormConf.get("HOST");
		influxDB = InfluxDBFactory.connect("http://195.203.56.22:8086", "admin", "admin");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] datas = line.split("\\|");
		if (datas.length < 10)
			return;
		pay(datas, collector);
	}

	/**
	 * 支付
	 * 
	 * @param datas
	 * @param collector
	 */
	private void pay(String[] datas, BasicOutputCollector collector) {
		if ("null".equals(datas[5]))
			datas[5] = "0";
		if ("null".equals(datas[6]))
			datas[6] = "0";
		if ("null".equals(datas[7]))
			datas[7] = "0";
		String dbName = "online";
		Point point1 = Point.measurement("pay")
				// .time(Long.valueOf(datas[9]) + Long.valueOf(host),
				// TimeUnit.MILLISECONDS)
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS).tag("host", datas[1]).tag("merId", datas[2])
				.tag("orderFlag", datas[4]).field("allTime", Integer.valueOf(datas[5]))
				.field("ecifTime", Integer.valueOf(datas[6])).field("icqTime", Integer.valueOf(datas[7])).build();
		logger.info("point1"+point1);
		influxDB.write(dbName, "default", point1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

	public void db() {

	}

}
