/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.kafka.consumer;

import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import com.lppz.util.kafka.consumer.BaseKafkaConsumer;
import com.lppz.util.kafka.consumer.KafkaConsumerConfig;
public abstract class BaseHbaseKafkaConsumer<T> extends BaseKafkaConsumer<T>
{   
	protected static final Log logger = LogFactory
			.getLog(BaseHbaseKafkaConsumer.class);
	final Properties props = new Properties();
	public void doInit(Configuration cf)
	{
			props.put(Constants.Consumer.zookeeper_connect,
					cf.get(Constants.Consumer.zookeeper_connect));
			props.put(Constants.Consumer.zookeeper_connection_timeout_ms,
					cf.get(Constants.Consumer.zookeeper_connection_timeout_ms));
			props.put(Constants.Consumer.zookeeper_session_timeout_ms,
					cf.get(Constants.Consumer.zookeeper_session_timeout_ms));
			props.put(Constants.Consumer.rebalance_backoff_ms,
					cf.get(Constants.Consumer.rebalance_backoff_ms));
			props.put(Constants.Consumer.auto_commit_interval_ms,
					cf.get(Constants.Consumer.auto_commit_interval_ms));
			props.put(Constants.Consumer.zookeeper_sync_time_ms,
					cf.get(Constants.Consumer.zookeeper_sync_time_ms));
			props.put(Constants.Consumer.topic_patition_num,
					cf.get(Constants.Consumer.topic_patition_num,"1"));
			try {
				init();
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage(),e);
			}
	}
	
	protected KafkaConsumerConfig initConfig(){
		if(props==null)
		return null;
		KafkaConsumerConfig config=new KafkaConsumerConfig();
		config.setZookeeperConnect(props.getProperty(Constants.Consumer.zookeeper_connect));
		config.setZookeeperConnectionTimeoutMs(props.getProperty(Constants.Consumer.zookeeper_connection_timeout_ms));
		config.setZookeeperSessionTimeoutMs(props.getProperty(Constants.Consumer.zookeeper_session_timeout_ms));
		config.setRebalanceBackoffMs(props.getProperty(Constants.Consumer.rebalance_backoff_ms));
		config.setAutoCommitIntervalMs(props.getProperty(Constants.Consumer.auto_commit_interval_ms));
		config.setZookeeperSyncTimeMs(props.getProperty(Constants.Consumer.zookeeper_sync_time_ms));
		config.setTopicPatitionNum(props.getProperty(Constants.Consumer.topic_patition_num));
		return config;
	}
}