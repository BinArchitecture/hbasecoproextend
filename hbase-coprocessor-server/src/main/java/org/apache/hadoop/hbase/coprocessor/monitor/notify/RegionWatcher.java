package org.apache.hadoop.hbase.coprocessor.monitor.notify;

import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.coprocessor.monitor.RegionMonitor;
import org.apache.hadoop.hbase.coprocessor.observer.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.util.curator.CuratorFrameworkUtils;
import com.lppz.util.curator.comm.NotiflyService;
import com.lppz.util.curator.listener.ZookeeperProcessListen;

public class RegionWatcher extends BaseWatcher implements NotiflyService {
	private static final Logger LOG = LoggerFactory.getLogger(RegionWatcher.class);
	public static final String path = "/hbase/rs";
	private Map<String,HostAndPort> hosts;
	public RegionWatcher(Map<String,HostAndPort> hosts, CuratorFramework curator, ZookeeperProcessListen zookeeperListen) {
		super(curator, zookeeperListen);
		this.hosts = hosts;
	}
	
	public void addListen() throws Exception{
		addListen(path, this);
		CuratorFrameworkUtils.pathChildCache(getCurator(), path, getZookeeperListen());
	}

	@Override
	public Object notiflyProcess(boolean write) throws Exception {
		List<String> list = getChildNames(path);
		LOG.info("testlog notiflyProcess {} list {}",path, list);
		new RegionMonitor(hosts).dealDeadNode(list);
		return null;
	}
}
