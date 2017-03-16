package com.ecloude.storm.bolt;

import java.util.Map;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @Title: DataWriteByHttpClient.java
 * @Package com.ecloude.storm.bolt
 * @Description: TODO(使用http请求发送数据)
 * @author wwl
 * @date 2016年11月7日 下午5:10:14
 * @version V1.0
 */
public class DataWriteByHttpClient extends BaseBasicBolt  {
	
	private static final long serialVersionUID = 5401470485706189288L;
	private static final Logger logger = Logger.getLogger("order");
	private String host;
	private String INFLUXDB_URL;
	private static final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
	private static final HttpConnectionManagerParams params = new HttpConnectionManagerParams();
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		INFLUXDB_URL = (String) stormConf.get("INFLUXDB_URL");
		host=INFLUXDB_URL+"/write?db=online";
		params.setConnectionTimeout(10000);
		params.setSoTimeout(60000);
		params.setDefaultMaxConnectionsPerHost(10);
		params.setMaxTotalConnections(200);
		connectionManager.setParams(params);
	}

	public static String send(String reqXml, String postUrl) {
		HttpClient httpClient = new HttpClient(connectionManager);
		PostMethod method = new PostMethod(postUrl);
		method.addRequestHeader("Content-Type", "application/json;charset=UTF-8");
		try {
			method.setRequestEntity(new StringRequestEntity(reqXml, null, "utf-8"));
			httpClient.executeMethod(method);
			String res = method.getResponseBodyAsString();
			Header locationHeader = method.getResponseHeader("Content-Type");
			String location = null;
			if (locationHeader != null) {
				location = locationHeader.getValue();
				logger.debug("The page was redirected to:" + location);
			} else {
				logger.error("Location field value is null.");
			}
			logger.info(res);
			return res;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		return null;
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String body = input.getString(0);
		logger.info("start "+body);
		send(body,host);
		logger.info("end "+body);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
