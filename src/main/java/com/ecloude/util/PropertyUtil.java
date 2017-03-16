package com.ecloude.util;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;



/**
 * 属性配置读取工具
 */
public class PropertyUtil {

	private static final Logger logger = Logger.getLogger(PropertyUtil.class);
	private static Properties pros = new Properties();

	// 加载属性文件
	static {
		try {
			InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties");
			pros.load(in);
		} catch (Exception e) {
			logger.error("load configuration error", e);
		}
	}

	/**
	 * 读取配置文中的属性值
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return pros.getProperty(key);
	}

}
