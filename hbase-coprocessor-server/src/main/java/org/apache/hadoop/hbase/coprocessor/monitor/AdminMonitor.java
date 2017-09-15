package org.apache.hadoop.hbase.coprocessor.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.coprocessor.observer.HostAndPort;
import org.apache.hadoop.hbase.coprocessor.util.ShellUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.util.kryo.KryoUtil;

public class AdminMonitor extends BaseMonitor {
	private static final Logger LOG = LoggerFactory
			.getLogger(AdminMonitor.class);
    public static String ZNODE = Constants.ZOOBINGOADMIN;
    private Map<String,HostAndPort> hosts;
    private static int initialDelay = 5;
    private static int period = 30;
    
    public AdminMonitor(Map<String,HostAndPort> hosts) throws IOException, InterruptedException {
    	super(null, null, initialDelay, period);
    	this.hosts = hosts;
	}
    
    public AdminMonitor(CuratorFramework zk, Map<String,HostAndPort> hosts) throws IOException, InterruptedException {
    	super(zk, ZNODE, initialDelay, period);
    	this.hosts = hosts;
    }
    
    public AdminMonitor(CuratorFramework zk, Map<String,HostAndPort> hosts, int initialDelay, int period) throws IOException, InterruptedException {
    	super(zk, ZNODE, initialDelay, period);
    	this.hosts = hosts;
    }

	@Override
	public void dealDeadNode(List<String> list) {
		List<String> livenodes = getLivenodes(list);
		List<HostAndPort> lostnodes = getLostnodes(livenodes);
		checkBingo(lostnodes);
	}

	private void checkBingo(List<HostAndPort> lostnodes) {
		if (CollectionUtils.isNotEmpty(lostnodes)) {
			for (HostAndPort hap : lostnodes) {
				if(nodeAlive(hap)){
					addNodeToZk(hap);
				}else{
					restartNode(hap);
				}
			}
		}
	}
	
	private boolean nodeAlive(HostAndPort hap) {
		TelnetClient tc = new TelnetClient();
        try {
			tc.connect(hap.getHost(), hap.getPort());
			tc.disconnect();
			return true;
		} catch (Exception e) {
			LOG.error("telnet exception", e);
		}
		return false;
	}
	
	private void addNodeToZk(HostAndPort hap) {
		byte[] data;
		try {
			LOG.warn("开始添加bingoserver节点到zk host {}", hap.getHost());
			data = KryoUtil.kyroSeriLize(hap.getHost()+":"+hap.getPort(), -1);
			createNodeEphemeralSequential(ZNODE + "/" + hap.getName(), data);
		} catch (Exception e) {
			LOG.error("增加bingo node到zk异常",e);
		}
	}

	private void restartNode(HostAndPort hap) {
		try {
			LOG.warn("开始重启bingoserver host {}", hap.getHost());
			ShellUtil.execute("startBingoServerSingle.sh " + hap.getHost());
		} catch (Exception e) {
			LOG.error(String.format("重启bingo服务异常,host:%s ", hap.getHost()),e);
		}
	}

	private List<String> getLivenodes(List<String> list) {
		List<String> livenodes = new ArrayList<>();
		for (String node : list) {
			livenodes.add(node.split("hconnection")[0]);
		}
		return livenodes;
	}

	private List<HostAndPort> getLostnodes(List<String> list) {
		List<HostAndPort> lostnodes = new ArrayList<>();
		for (Entry<String, HostAndPort> entry : hosts.entrySet()) {
			if (list.indexOf(entry.getKey()) == -1) {
				lostnodes.add(entry.getValue());
			}
		}
		return lostnodes;
	}
}
