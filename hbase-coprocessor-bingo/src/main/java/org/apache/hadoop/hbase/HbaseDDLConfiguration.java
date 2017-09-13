package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BaseClientUtil;
import org.apache.hadoop.hbase.client.coprocessor.IdxHbaseClient;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.lppz.configuration.hbase.HBaseConfigYamlBean;

@Configuration
public class HbaseDDLConfiguration {
	private static final Logger LOG = LoggerFactory
			.getLogger(HbaseDDLConfiguration.class);

	@Bean(name = "abstractHbaseDDLClient")
	public AbstractHbaseDDLClient abstractHbaseDDLClient() throws IOException {
		AbstractHbaseDDLClient client = new AbstractHbaseDDLClient();
		try {
			org.apache.hadoop.conf.Configuration cf=initConf();
			AbstractHbaseDDLClient.init(cf);
			HbaseWatchClient.setZk(IdxHbaseClient.getRz());
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return client;
	}
	
	public synchronized org.apache.hadoop.conf.Configuration initConf(String... yaml)
			throws Exception {
		String yypath = yaml == null || yaml.length == 0 ? null : yaml[0];
		HBaseConfigYamlBean hbaseBean = BaseClientUtil.initHBaseConfig(yypath);
		if (hbaseBean == null)
			throw new Exception("lack of hbaseYaml config File");
		String clientPort = hbaseBean.getZkClientPort();
		String quorum = hbaseBean.getZkQuorum();
		if (clientPort == null || quorum == null)
			throw new Exception("config properties has error!");
		Long timeout = hbaseBean.getHbaseRpcTimeOut();
		Integer period = hbaseBean.getHbaseScanTimeoutPeriod();
		Long caching = hbaseBean.getHbaseClientScannerCaching();
		org.apache.hadoop.conf.Configuration conf = BaseClientUtil.initConf(clientPort, quorum, timeout, period,
				caching);
		if(hbaseBean.getEsBean()!=null){
			BaseClientUtil.putEsBean2Zk(conf, hbaseBean.getEsBean());
		}
		return conf;
	}
}