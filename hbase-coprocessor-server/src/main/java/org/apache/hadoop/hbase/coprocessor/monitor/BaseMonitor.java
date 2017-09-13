package org.apache.hadoop.hbase.coprocessor.monitor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMonitor {
	private static final Logger LOG = LoggerFactory
			.getLogger(BaseMonitor.class);
    private String nodePath;
//    private RecoverableZooKeeper zk;
    private int initialDelay;
    private int period;
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
    private CuratorFramework zk;
    
    public BaseMonitor(CuratorFramework zk, String nodePath, int initialDelay, int period) throws IOException, InterruptedException {
    	this.zk = zk;
    	this.nodePath = nodePath;
    	this.initialDelay = initialDelay;
    	this.period = period;
	}
    
    public void startMonitor(){
    	executor.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				LOG.info("start to check monitor path {}",nodePath);
				List<String> list;
				try {
					list = zk.getChildren().forPath(nodePath);
					dealDeadNode(list);
				} catch (KeeperException e) {
					LOG.error(e.getMessage(), e);
				} catch (InterruptedException e) {
					LOG.error(e.getMessage(), e);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
    }
    
	public void createNodeEphemeralSequential(String path, byte[] data) throws Exception{
//		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		zk.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(Ids.OPEN_ACL_UNSAFE).forPath(path,	data);
	}

	public abstract void dealDeadNode(List<String> list);
	
}
