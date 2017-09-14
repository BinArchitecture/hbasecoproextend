package org.apache.hadoop.hbase.coprocessor.monitor.notify;

import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.coprocessor.monitor.AdminMonitor;
import org.apache.hadoop.hbase.coprocessor.observer.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.util.curator.CuratorFrameworkUtils;
import com.lppz.util.curator.comm.NotiflyService;
import com.lppz.util.curator.listener.ZookeeperProcessListen;

public class BingoWatcher extends BaseWatcher implements NotiflyService {
	private static final Logger LOG = LoggerFactory.getLogger(BingoWatcher.class);
	public static final String path = Constants.ZOOBINGOADMIN;
	private Map<String,HostAndPort> hosts;
	public BingoWatcher(Map<String,HostAndPort> hosts, CuratorFramework curator, ZookeeperProcessListen zookeeperListen) {
		super(curator, zookeeperListen);
		this.hosts = hosts;
	}
	
	public void addListen() throws Exception{
		addListen(path, this);
		CuratorFrameworkUtils.pathChildCache(getCurator(), path, getZookeeperListen());
	}

	@Override
	public Object notiflyProcess(boolean write) throws Exception {
		new AdminMonitor(hosts).dealDeadNode(getChildNames(path));
		return null;
	}
}
