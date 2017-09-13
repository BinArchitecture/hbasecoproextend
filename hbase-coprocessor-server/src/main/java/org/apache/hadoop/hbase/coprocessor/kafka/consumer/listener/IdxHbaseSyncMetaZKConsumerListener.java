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
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.IdxHbaseSyncMetaZKConsumerListenerException;
import org.apache.hadoop.hbase.client.coprocessor.model.ZkMetaIdxData;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg.Operate;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSON;

/**
 *
 */
public class IdxHbaseSyncMetaZKConsumerListener extends
		IdxHbaseSyncMetaMapConsumerListener {
	protected static final Log logger = LogFactory
			.getLog(IdxHbaseSyncMetaZKConsumerListener.class);
	private MetaTableIndex metaIndex;
	private RecoverableZooKeeper rz;

	public IdxHbaseSyncMetaZKConsumerListener(MetaTableIndex metaIndex,
			RecoverableZooKeeper rz) {
		this.metaIndex = metaIndex;
		this.rz = rz;
	}

	@Override
	protected void doMsg(IdxKafkaMsg msg) {
		try {
			logger.info(JSON.toJSONString(msg));
			if (!init(msg, metaIndex))
				return;
			ZkMetaIdxData zk = null;
			if (Operate.ADD.equals(msg.getOp())) {
				zk = handleAddMetaIdx(msg, metaIndex);
			} else {
				zk = handleRemoveMetaIdx(msg, metaIndex);
			}

			rz.setData(Constants.ZOOINDEXPATH
					+ Constants.ZOOINDEXTBINDEXMAPPATH + "/" + msg.getTbName()
					+ "/" + msg.getFamilyName(), JSON.toJSONString(zk.getMfi())
					.getBytes(), -1);
			rz.setData(
					Constants.ZOOINDEXPATH
							+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH + "/"
							+ msg.getTbName() + "/" + msg.getFamilyName(), JSON
							.toJSONString(zk.getMfpi()).getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e.getMessage(), e);
			try {
				throw new IdxHbaseSyncMetaZKConsumerListenerException(e.getMessage());
			} catch (IdxHbaseSyncMetaZKConsumerListenerException e1) {
			}
		}
	}
}