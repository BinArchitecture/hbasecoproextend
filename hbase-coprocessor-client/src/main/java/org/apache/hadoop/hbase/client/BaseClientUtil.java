package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.client.coprocessor.util.ReFelctionUtil;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.elasticsearch.client.support.AbstractClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;

import com.lppz.configuration.es.EsBaseYamlBean;
import com.lppz.configuration.hbase.HBaseConfigYamlBean;
import com.lppz.elasticsearch.EsClientUtil;
import com.lppz.elasticsearch.LppzEsComponent;
import com.lppz.util.JmxGetIPPort;
import com.lppz.util.kryo.KryoUtil;

public class BaseClientUtil {
	private static final Logger logger = LoggerFactory
			.getLogger(BaseClientUtil.class);

	public static Configuration initConf(String clientPort, String quorum,
			Long timeout, Integer period, Long caching) {
		//Configuration conf = HBaseConfiguration.create();
		Configuration conf = HbaseUtil.createConf(clientPort, quorum, timeout, period, caching);
		return conf;
	}

	public static HBaseConfigYamlBean initHBaseConfig(String yamlPath)
			throws IOException {
		HBaseConfigYamlBean bean = loadYaml(
				yamlPath == null ? "/META-INF/hbase-lppz-client.yaml"
						: yamlPath, yamlPath == null);
		initEs4HBase(bean);
		return bean;
	}

	public static void initEs4HBase(HBaseConfigYamlBean bean) {
		AbstractClient client = null;
		if (bean != null) {
			if(bean.getEsBean()!=null)
			client = EsClientUtil.buildPoolClientProxy(bean.getEsBean());
		}
		LppzEsComponent.getInstance().setClient(client);
	}

	public static HBaseConfigYamlBean loadYaml(String yaml, boolean mark) {
		Resource res = null;
		if (mark)
			res = new ClassPathResource(yaml);
		else
			res = new FileSystemResource(yaml);
		if ((res == null) || (!(res.exists()))) {
			return null;
		}
		HBaseConfigYamlBean bean = null;
		try {
			bean = (HBaseConfigYamlBean) new Yaml().loadAs(
					res.getInputStream(), HBaseConfigYamlBean.class);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return bean;
	}
	
	public static Connection getConnFromHbaseConf(Configuration cfg) {
		Connection conn=null;
		try {
			conn = ConnectionFactory.createConnection(cfg);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return conn;
	}
	
	public static RecoverableZooKeeper getZkFromHbaseConf(Configuration cfg,Connection... connn) {
		Connection conn=null;
		if(connn==null||connn.length==0)
			conn=getConnFromHbaseConf(cfg);
		else
			conn=connn[0];
		ZooKeeperWatcher keepAliveZookeeper=null;
		try {
			keepAliveZookeeper = ReFelctionUtil
					.getDynamicObj(conn.getClass(),
							"keepAliveZookeeper", conn);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		RecoverableZooKeeper rz=null;
		if (keepAliveZookeeper != null) {
			rz = keepAliveZookeeper.getRecoverableZooKeeper();
		}
		return rz;
	}
	
	public static EsBaseYamlBean getEsBeanFromZk(RecoverableZooKeeper rz){
		if(rz==null)
			return null;
		try {
			List<String> paths=rz.getChildren(Constants.ZOOBINGOES, false);
			if(CollectionUtils.isNotEmpty(paths)){
				for(String path:paths){
					byte[] b=rz.getData(Constants.ZOOBINGOES+"/"+path, true, null);
					if(b!=null)
						return KryoUtil.kyroDeSeriLize(b, EsBaseYamlBean.class);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return null;
	}
	
	public static void putEsBean2Zk(Configuration cfg,EsBaseYamlBean bean){
		if(bean==null) return;
		RecoverableZooKeeper rz=getZkFromHbaseConf(cfg);
		try {
			if(rz.exists(Constants.ZOOBINGOES, true)==null){
				rz.create(Constants.ZOOBINGOES, null,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if(rz.exists(Constants.ZOOBINGOADMIN, true)==null)
				rz.create(Constants.ZOOBINGOADMIN, null,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			String host=JmxGetIPPort.getMainIPPort();
			String hostname = InetAddress.getLocalHost().getHostName();
			byte[] bb=KryoUtil.kyroSeriLize(host, -1);
			byte[] b=KryoUtil.kyroSeriLize(bean, -1);
			rz.create(Constants.ZOOBINGOES+"/"+hostname, b,
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			rz.create(Constants.ZOOBINGOADMIN+"/"+hostname, bb,
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}