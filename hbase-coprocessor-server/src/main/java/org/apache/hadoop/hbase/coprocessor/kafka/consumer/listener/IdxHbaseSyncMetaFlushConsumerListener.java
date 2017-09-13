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
package org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.IdxHbaseSyncMetaFlushConsumerListenerException;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;

import com.alibaba.fastjson.JSON;
import com.lppz.util.kafka.consumer.listener.BaseKafkaConsumerListener;

/**
 *
 */
public class IdxHbaseSyncMetaFlushConsumerListener extends BaseKafkaConsumerListener<MetaTableIndex>
{
	protected static final Log logger = LogFactory
			.getLog(IdxHbaseSyncMetaFlushConsumerListener.class);
	private MetaTableIndex metaIndex;
	public IdxHbaseSyncMetaFlushConsumerListener(MetaTableIndex metaIndex){
		this.metaIndex=metaIndex;
	}
	public IdxHbaseSyncMetaFlushConsumerListener(){
	}
	@Override
	protected void doMsg(MetaTableIndex msg) {
		if(msg!=null){
			try {
				logger.info(JSON.toJSONString(msg));
				this.metaIndex.setTbIndexMap(msg.getTbIndexMap());
				this.metaIndex.setTbIndexNameMap(msg.getTbIndexNameMap());
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
				try {
					throw new IdxHbaseSyncMetaFlushConsumerListenerException(e.getMessage());
				} catch (IdxHbaseSyncMetaFlushConsumerListenerException e1) {
				}
			}
		}
	}
}
