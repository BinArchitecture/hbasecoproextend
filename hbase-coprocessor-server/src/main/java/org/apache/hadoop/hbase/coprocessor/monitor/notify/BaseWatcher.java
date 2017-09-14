package org.apache.hadoop.hbase.coprocessor.monitor.notify;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.lppz.util.curator.comm.NotiflyService;
import com.lppz.util.curator.listener.ZookeeperProcessListen;

public class BaseWatcher {
	 private CuratorFramework curator;
	 private ZookeeperProcessListen zookeeperListen;
	 
	 public BaseWatcher(CuratorFramework curator, ZookeeperProcessListen zookeeperListen) {
		 this.curator = curator;
		 this.zookeeperListen = zookeeperListen;
	}

	public CuratorFramework getCurator() {
		return curator;
	}

	public ZookeeperProcessListen getZookeeperListen() {
		return zookeeperListen;
	}

	protected void addListen(String key, NotiflyService cacheNotiflySercie) {
		zookeeperListen.addListen(key, cacheNotiflySercie);
    }
	
	protected List<String> getChildNames(String path) throws Exception {
        return curator.getChildren().forPath(path);
    }
	
	protected void createNodeEphemeralSequential(String path, byte[] data) throws Exception{
		curator.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(Ids.OPEN_ACL_UNSAFE).forPath(path,	data);
	}
	 
}
