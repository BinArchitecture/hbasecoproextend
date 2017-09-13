/*
 *
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
package org.apache.hadoop.hbase.coprocessor.kafka.producer;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.coprocessor.Constants;

import com.alibaba.fastjson.JSON;

public abstract class BaseKafkaProducer<T>  {
	protected static final Log logger = LogFactory
			.getLog(BaseKafkaProducer.class);
	private String topic;
	private String batch_num_messages;
	private String producer_type;
	private String queue_buffering_max_ms;

	public String getBatch_num_messages() {
		return batch_num_messages;
	}

	public void setBatch_num_messages(String batch_num_messages) {
		this.batch_num_messages = batch_num_messages;
	}

	public String getProducer_type() {
		return producer_type;
	}

	public void setProducer_type(String producer_type) {
		this.producer_type = producer_type;
	}

	public String getQueue_buffering_max_ms() {
		return queue_buffering_max_ms;
	}

	public void setQueue_buffering_max_ms(String queue_buffering_max_ms) {
		this.queue_buffering_max_ms = queue_buffering_max_ms;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	private Producer<String, String> producer;

	protected void initProducer(Configuration cf) {
		try {
			final Properties props = new Properties();
			props.put(Constants.Producer.batch_num_messages, batch_num_messages != null ? batch_num_messages : cf.get(Constants.Producer.batch_num_messages));
			props.put(Constants.Producer.metadata_broker_list, cf.get(Constants.Producer.metadata_broker_list));
			props.put(Constants.Producer.producer_type, producer_type != null ? producer_type : cf.get(Constants.Producer.producer_type));
			props.put(Constants.Producer.queue_buffering_max_ms,
					queue_buffering_max_ms != null ? queue_buffering_max_ms : cf.get(Constants.Producer.queue_buffering_max_ms));
			props.put(Constants.Producer.request_required_acks, cf.get(Constants.Producer.request_required_acks));
			props.put(Constants.Producer.serializer_class, cf.get(Constants.Producer.serializer_class));
			props.put(Constants.Producer.queue_enqueue_timeout_ms, "-1");
			final ProducerConfig producerConfig = new ProducerConfig(props);
			producer = new Producer<String, String>(producerConfig);
		} catch (final Exception e) {
			logger.error("init SysLogProducer exception:", e);
			throw e;
		} catch (final Error e) {
			logger.error("init SysLogProducer exception:", e);
			throw e;
		}
	}
	
	public boolean sendMsg(final T t) {
		if (t == null) {
			return false;
		}
		try {
			final String jmsg = JSON.toJSONString(t);
			final KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, jmsg);
			producer.send(msg);
			return true;

		} catch (final Exception e) {
			logger.error("send msg to jump mq exception:", e);
			throw e;
		} catch (final Error e) {
			logger.error("send msg to jump mq error:", e);
			throw e;
		}
	}

	protected Producer<String, String> getProducer() {
		return producer;
	}
}
