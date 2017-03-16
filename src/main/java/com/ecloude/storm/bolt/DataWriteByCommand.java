package com.ecloude.storm.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


/**
 * @Title: DataWrite.java
 * @Package com.mysoft.storm.bolt
 * @Description: TODO(数据序列化,通过执行linux命令向influxdb发送数据)
 * @author wwl
 * @date 2016年7月18日 下午6:15:29
 * @version V1.0
 */
public class DataWriteByCommand extends BaseBasicBolt {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger("order");

	private static final long serialVersionUID = 5683648523524179434L;


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String str = input.getString(0);
		logger.info("request data:"+str);
		try {
			String[] command = { "/bin/sh", "-c", str };
			Process ps = Runtime.getRuntime().exec(command);
			BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			StringBuffer sb = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			String result = sb.toString();
			int i=result.indexOf("{");
			if(i!=-1){
				logger.error(result.substring(i));
			}
		} catch (IOException e) {
			logger.error("execute(Tuple, BasicOutputCollector)", e);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
