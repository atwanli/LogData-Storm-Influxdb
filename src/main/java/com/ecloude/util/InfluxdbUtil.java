package com.ecloude.util;

import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

/**  
* @Title: InfluxdbUtil.java
* @Package com.ecloude.util
* @Description: TODO()
* @author wwl 
* @date 2016年11月8日 下午1:40:46
* @version V1.0  
*/
public class InfluxdbUtil {

	public static void main(String[] args) {
		save();
	}

	public static void save(){
		InfluxDB influxDB=InfluxDBFactory.connect("http://195.203.56.22:8086", "admin", "admin");  
	    String dbName = "online";  
//	    influxDB.createDatabase(dbName);  
	      
//	    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);  
	      
	    Point point1 = Point.measurement("pay")  
//	                        .time(Long.valueOf(datas[9]) + Long.valueOf(host), TimeUnit.MILLISECONDS) 
	                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)  
	                        .tag("host", "195.203.56.126") 
	                        .tag("merId", "123456789") 
	                        .tag("orderFlag", "1") 
	                        .field("allTime", 2200) 
	                        .field("ecifTime", 500) 
	                        .field("icqTime", 300) 
	                        .build();  
	    influxDB.write(dbName, "default", point1);  
	}
}

