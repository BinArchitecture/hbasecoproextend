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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.HbaseKafkaInitException;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;

import com.lppz.util.kafka.constant.KafkaConstant;
import com.lppz.util.kafka.producer.BaseKafkaProducer;
import com.lppz.util.kafka.producer.KafkaProducerConfig;
import com.lppz.util.kafka.producer.async.BaseMsgAsyncKafkaHandler;

public class IdxHBaseKafkaEsRegionServerProducer extends BaseKafkaProducer<EsIdxHbaseType> {
	public IdxHBaseKafkaEsRegionServerProducer(Configuration cf) throws HbaseKafkaInitException{
		super.setProducer_type(KafkaConstant.ASYNC);
		super.setProduce_serilize_class(KafkaConstant.BYTEENCODER);
		kafkaProducerConfig=buildConfig(cf);
		try {
			BaseMsgAsyncKafkaHandler.batch_num_messages=Integer.parseInt(kafkaProducerConfig.getBatch_num_messages());
			BaseMsgAsyncKafkaHandler.queue_buffering_max_ms=Long.parseLong(kafkaProducerConfig.getQueue_buffering_max_ms());
			super.init();
		} catch (Exception e) {
			throw new HbaseKafkaInitException(e.getMessage());
		}
	}

	private KafkaProducerConfig buildConfig(Configuration cf) {
		final Properties props = new Properties();
		props.put(Constants.Producer.batch_num_messages, cf.get(Constants.Producer.batch_num_messages));
		props.put(Constants.Producer.metadata_broker_list, cf.get(Constants.Producer.metadata_broker_list));
		props.put(Constants.Producer.queue_buffering_max_ms,cf.get(Constants.Producer.queue_buffering_max_ms));
		props.put(Constants.Producer.request_required_acks, cf.get(Constants.Producer.request_required_acks));
		props.put(Constants.Producer.compression_codec,"snappy");
		props.put(Constants.Producer.compressed_topics,Constants.KAFKASYNCESREGIONSERVERSYNC_TOPIC);
		props.put(Constants.Producer.message_send_max_retries,10);
		props.put(Constants.Producer.retry_backoff_ms,300);
		props.put(Constants.Producer.request_required_acks, cf.get(Constants.Producer.request_required_acks));
		KafkaProducerConfig kprc= new KafkaProducerConfig(props);
		int cSize=cf.getInt(Constants.Producer.async_thread_num, 1);
		kprc.setKafkaClusterSize(cSize>=8?8:cSize);
		return kprc;
	}

	@Override
	protected void resetTopic() {
		super.setTopic(Constants.KAFKASYNCESREGIONSERVERSYNC_TOPIC);
	}
}
