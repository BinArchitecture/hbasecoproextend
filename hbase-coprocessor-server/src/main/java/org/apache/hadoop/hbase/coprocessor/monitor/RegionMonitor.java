package org.apache.hadoop.hbase.coprocessor.monitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.coprocessor.util.ShellUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionMonitor extends BaseMonitor {
	private static final Logger LOG = LoggerFactory
			.getLogger(RegionMonitor.class);
    public static final String ZNODE = "/hbase/rs";
    private static int initialDelay = 5;
    private static int period = 30;
    private List<String> regionservers;
    
    public RegionMonitor(CuratorFramework zk) throws IOException, InterruptedException {
    	super(zk, ZNODE, initialDelay, period);
    	this.regionservers = fetchHostfromSlaveFile();
	}
    public RegionMonitor(CuratorFramework zk, int initialDelay, int period) throws IOException, InterruptedException {
    	super(zk, ZNODE, initialDelay, period);
    	this.regionservers = fetchHostfromSlaveFile();
    }
    
	private List<String> fetchHostfromSlaveFile() {
	    String path=System.getenv("HBASE_HOME")+"/conf/regionservers";
        List<String> rtnList = new ArrayList<String>();

        FileReader reader = null;
        BufferedReader br = null;
        try {
        	reader = new FileReader(path);
        	br = new BufferedReader(reader);
        	String str = null;
        	while((str = br.readLine()) != null) {
        		rtnList.add(str);
        	}
			return rtnList;
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}finally{
			if (br != null) {				
				try {
					br.close();
				} catch (IOException e) {
					LOG.error("关闭hbase regionservers文件bufferReader异常", e);
				}
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.error("关闭hbase regionservers文件reader异常", e);
				}
			}
			
		}
		return null;
	}
	
	@Override
	public void dealDeadNode(List<String> list) {
		List<String> lostnodes = new ArrayList<String>();
		List<String> livenodes = new ArrayList<String>();
	
		for (String n:list) {
			livenodes.add(n.split(",")[0]);
		}
		
		for (int i=0; i<regionservers.size();i++) {
			String node = regionservers.get(i);
			if (livenodes.contains(node)) {
				continue;
			}else {
				lostnodes.add(node);
			}
		}
		if (CollectionUtils.isNotEmpty(lostnodes)) {
			for (String child:lostnodes) {
				try {
					ShellUtil.execute("restartRegion.sh "+ child);
				} catch (IOException | InterruptedException | TimeoutException e) {
					LOG.error("重启region异常",e);
				}
			}
		}
	}
}
