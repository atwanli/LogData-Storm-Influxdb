package com.ecloude.storm.spout;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;



/**  
* @Title: LogReader.java
* @Package cn.itcast.storm.spout
* @Description: TODO(读取日志)
* @author wwl
* @date 2016年7月18日 下午6:05:58
* @version V1.0  
*/
public class LogReader extends BaseRichSpout {
	private static final Logger logger = Logger.getLogger("order");

	private static final long serialVersionUID = 2197521792014017918L;
	
	private SpoutOutputCollector collector;
	private long lastTimeFileSize = 0;
	
	/**
	 * @Fields fixFileName : TODO[N](固定日志文件前缀名time.log)
	 */ 
	private String fixFileName;
	
	/**
	 * @Fields fileName : TODO[N](当前读取的文件名 格式：time.log.yyyymmdd)
	 */ 
	private String fileName;
	private File file;
	private SimpleDateFormat sdf = new SimpleDateFormat(".yyyyMMdd");
	
	/**
	 * @Fields fileFlag : TODO[N](读取文件标识1-只读指定文件0-读完指定文件后继续读取当前日志文件)
	 */ 
	private boolean fileFlag=false;

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		String fileNameTmp = (String) conf.get("FILENAME");
		String flag = (String) conf.get("FILEFLAG");
		if(fileNameTmp.endsWith(".log")){
			fixFileName=fileNameTmp;
			fileName=fileNameTmp+sdf.format(new Date());
		}else{
			fixFileName=fileNameTmp.substring(0, fileNameTmp.lastIndexOf("."));
			fileName=fileNameTmp;
		}
		fileFlag=Boolean.valueOf(flag);
		logger.info("--------------start--------------");
		logger.info("fileName:"+fileName);
		logger.info("fixFileName :"+fixFileName);
		logger.info("fileFlag :"+fileFlag);
		logger.info("--------------end--------------");
		file = new File(fileName);
		
	}

	@Override
	public void nextTuple() {
		work();
		if(fileFlag) return;
		rollOver();
	}
	
	public void rollOver(){
	    String datedFilename = fixFileName+sdf.format(new Date());;
	    if (fileName.equals(datedFilename)) {
	      return;
	    }
	    work();
	    lastTimeFileSize=0;
	    fileName=datedFilename;
	    logger.info("start by file:"+fileName);
	    file = new File(fileName);
	}
	/**
	 * 读取当天日志
	 */
	public void work() {
		if(!file.exists()){
			logger.error("-----日志文件：["+fileName+"]不存在-----");
			return;
		};
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(file, "r");
			randomFile.seek(lastTimeFileSize);
			String line = null;
			StringBuffer sb=new StringBuffer();
			while ((line = randomFile.readLine()) != null) {
				if(line.contains("END")){
					collector.emit(new Values(line));
					sb.append(line);
					lastTimeFileSize=lastTimeFileSize+line.getBytes().length;
					logger.info("readline " + line+"  "+lastTimeFileSize);
				}
			}
			randomFile.close();
		} catch (Exception e) {
			logger.error("work()", e);
			e.printStackTrace();
		}

	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
