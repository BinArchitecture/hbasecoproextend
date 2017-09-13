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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.IdxHbaseLockedConsumerListenerException;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg.Operate;

import com.alibaba.fastjson.JSON;
import com.lppz.util.kafka.consumer.listener.BaseKafkaConsumerListener;

/**
 *
 */
public class IdxHbaseLockedConsumerListener extends BaseKafkaConsumerListener<IdxKafkaMsg>
{
	protected static final Log logger = LogFactory
			.getLog(IdxHbaseLockedConsumerListener.class);
	private Map<String,Map<String,Boolean>> lockedIdxMap;
	public IdxHbaseLockedConsumerListener(Map<String,Map<String,Boolean>> lockedIdxMap){
		this.lockedIdxMap=lockedIdxMap;
	}
	public IdxHbaseLockedConsumerListener(){
	}
	@Override
	protected void doMsg(IdxKafkaMsg msg) {
		try {
			logger.info(JSON.toJSONString(msg));
			String tBName=msg.getTbName();
			String familyName=msg.getFamilyName();
			if(lockedIdxMap.containsKey(tBName)){
				if(lockedIdxMap.get(tBName).containsKey(familyName)){
					if(Operate.ADDIDXLOCK.equals(msg.getOp())||Operate.RELEASEIDXLOCK.equals(msg.getOp()))
					lockedIdxMap.get(tBName).put(familyName,Operate.ADDIDXLOCK.equals(msg.getOp()));
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			try {
				throw new IdxHbaseLockedConsumerListenerException(e.getMessage());
			} catch (IdxHbaseLockedConsumerListenerException e1) {
			}
		}
	}
}
