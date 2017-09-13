package org.apache.hadoop.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

//@Component
//@DependsOn("abstractHbaseDDLClient")
public class HbaseWatchClient implements InitializingBean {
	private static final Logger LOG = LoggerFactory
			.getLogger(HbaseWatchClient.class);
    public static final String ZNODE = "/hbase/rs";
    private static RecoverableZooKeeper zk;
    
	public static RecoverableZooKeeper getZk() {
		return zk;
	}

	public static void setZk(RecoverableZooKeeper zk) {
		HbaseWatchClient.zk = zk;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		
		new Thread(new Runnable(){
			private void RunShell(String hostname) {   
		        try {  
		            String shpath="restartRegion.sh "+ hostname;  
		            Process ps = Runtime.getRuntime().exec(shpath);  
		            ps.waitFor();  
		            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));  
		            StringBuffer sb = new StringBuffer();  
		            String line;  
		            while ((line = br.readLine()) != null) {  
		                sb.append(line).append("\n");  
		            }  
		            String result = sb.toString();  
		            LOG.info(result);  
		            }   
		        catch (Exception e) {  
		        	LOG.warn(e.getMessage());  
		            }  
		    }
			
			@Override
			public void run() {
				Watcher wc = new Watcher() {
					@Override
					public void process(WatchedEvent event) {
					//LOG.info("start to watch hbase regionserver path");
					try {
						//String zkData = new String(zk.getData(ZNODE, true, null));
						List<String> lostnodes = new ArrayList<String>();
						List<String> livenodes = new ArrayList<String>();
						List<String> list = zk.getChildren(ZNODE, true);
					
						for (String n:list) {
							livenodes.add(n.split(",")[0]);
						}
						
						List<String> regionservers = fetchHostfromSlaveFile();
						
						for (int i=0; i<regionservers.size();i++) {
							String node = regionservers.get(i);
							if (livenodes.contains(node)) {
								continue;
							}else {
								lostnodes.add(node);
							}
						}
						for (String child:lostnodes) {
							LOG.info("节点"+child+"退出了，重新启动");
							RunShell(child);
						}
						zk.getChildren(ZNODE, true);
						//LOG.info(zkData);
					} catch (KeeperException e) {
						LOG.warn(e.getMessage());
					} catch (InterruptedException e) {
						LOG.warn(e.getMessage());
					}
					}

					private List<String> fetchHostfromSlaveFile() {
					    String path=System.getenv("HBASE_HOME")+"/conf/regionservers";
				        InputStreamReader isr = null;
				        List<String> rtnList = new ArrayList<String>();
						try {
							isr = new InputStreamReader(new FileInputStream(new File(path)),"UTF-8");
						} catch (UnsupportedEncodingException e) {
							LOG.warn(e.getMessage());
						} catch (FileNotFoundException e) {
							LOG.warn(e.getMessage());
						}
				        char[]  c=new char[1024];
				        try {
							while((isr.read(c, 0, c.length))!=-1){
						    rtnList.add(c.toString());
							}
							return rtnList;
						} catch (IOException e) {
							LOG.warn(e.getMessage());
						}
				        //关闭读取流
				        try {
							isr.close();
						} catch (IOException e) {
							LOG.warn(e.getMessage());
						}
						return null;
					}
				};
			}
		});
	}
}
