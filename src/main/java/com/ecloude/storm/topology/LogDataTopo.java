package com.ecloude.storm.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.ecloude.storm.bolt.DataWriteByCommand;
import com.ecloude.storm.bolt.LogSpliter;
import com.ecloude.storm.bolt.LogSpliter2Http;
import com.ecloude.storm.bolt.LogSpliter3Database;
import com.ecloude.storm.spout.LogReader;
import com.ecloude.util.PropertyUtil;

/**
 * @Title: LogDataTopo.java
 * @Package com.mysoft.storm.topology
 * @Description: TODO(处理日志，并将数据保存)
 * @author wwl
 * @date 2016年7月18日 下午6:19:19
 * @version V1.0
 */
public class LogDataTopo {

	private static final Logger logger = Logger.getLogger(LogDataTopo.class);

	public static void main(String[] args) {
		if (args.length != 5) {
			System.err.println(
					"such as : java -jar  [jar包]  [日志文件路径] [1/0(1-only one file)] [主机编号[1-4]] [spilter] [write]");
			System.err.println("such as : java -jar  WordCount.jar  D://monitor.log 0 1 4 8");
			System.exit(2);
		}
		String fileName = args[0];
		String fileFlag = args[1];
		String host = args[2];
		String spilterNum = args[3];
		String writeNum = args[4];
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("log-reader", new LogReader());
//		builder.setBolt("log-spilter", new LogSpliter2Http(), Integer.valueOf(spilterNum)).shuffleGrouping("log-reader");
//		builder.setBolt("data-write", new DataWriteByHttpClient(), Integer.valueOf(writeNum))
		builder.setBolt("log-spilter", new LogSpliter3Database(), Integer.valueOf(spilterNum)).shuffleGrouping("log-reader");
//		builder.setBolt("log-spilter", new LogSpliter(), Integer.valueOf(spilterNum)).shuffleGrouping("log-reader");
//		builder.setBolt("data-write", new DataWriteByCommand(), Integer.valueOf(writeNum))
//		.shuffleGrouping("log-spilter");
		Config conf = new Config();
		conf.setNumWorkers(1); 
		conf.put("FILENAME", fileName);
		conf.put("FILEFLAG", fileFlag);
		conf.put("INFLUXDB_URL", PropertyUtil.getProperty("influxdb_url"));
		conf.put("HOST", host);
		conf.setDebug(false);
		 LocalCluster cluster = new LocalCluster();
		 cluster.submitTopology("LogData", conf, builder.createTopology());
//		try {
//			logger.info("================start running================");
//			StormSubmitter.submitTopology("LogData", conf, builder.createTopology());
//			logger.info("================end running ok================");
//		} catch (AuthorizationException e) {
//			e.printStackTrace();
//		} catch (AlreadyAliveException e1) {
//			e1.printStackTrace();
//		} catch (InvalidTopologyException e2) {
//			e2.printStackTrace();
//		}
	}
}
