package org.apache.hadoop.hbase.coprocessor.monitor.notify;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.coprocessor.monitor.RegionMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.util.curator.CuratorFrameworkUtils;
import com.lppz.util.curator.comm.NotiflyService;
import com.lppz.util.curator.listener.ZookeeperProcessListen;

public class RegionWatcher extends BaseWatcher implements NotiflyService {
	private static final Logger LOG = LoggerFactory.getLogger(RegionWatcher.class);
	public static final String path = "/hbase/rs";
	public RegionWatcher(CuratorFramework curator, ZookeeperProcessListen zookeeperListen) {
		super(curator, zookeeperListen);
	}
	
	public void addListen() throws Exception{
		LOG.info("testlog addListen {}",path);
		addListen(path, this);
		CuratorFrameworkUtils.pathChildCache(getCurator(), path, getZookeeperListen());
	}

	@Override
	public Object notiflyProcess(boolean write) throws Exception {
		LOG.info("testlog notiflyProcess {}",path);
		new RegionMonitor().dealDeadNode(getChildNames(path));
		return null;
	}
}
