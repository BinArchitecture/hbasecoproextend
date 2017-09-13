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
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.IdxHbaseSyncMetaMapConsumerListenerException;
import org.apache.hadoop.hbase.client.coprocessor.model.ZkMetaIdxData;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg.Operate;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyPosIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;

import com.alibaba.fastjson.JSON;
import com.lppz.util.kafka.consumer.listener.BaseKafkaConsumerListener;

/**
 *
 */
public class IdxHbaseSyncMetaMapConsumerListener extends BaseKafkaConsumerListener<IdxKafkaMsg>
{
	protected static final Log logger = LogFactory
			.getLog(IdxHbaseSyncMetaMapConsumerListener.class);
	private MetaTableIndex metaIndex;
	public IdxHbaseSyncMetaMapConsumerListener(MetaTableIndex metaIndex){
		this.metaIndex=metaIndex;
	}
	public IdxHbaseSyncMetaMapConsumerListener(){
	}
	@Override
	protected void doMsg(IdxKafkaMsg msg) {
		try {
			logger.info(JSON.toJSONString(msg));
			handleMetaIdx(msg,metaIndex);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			try {
				throw new IdxHbaseSyncMetaMapConsumerListenerException(e.getMessage());
			} catch (IdxHbaseSyncMetaMapConsumerListenerException e1) {
			}
		}
	}
	protected void handleMetaIdx(IdxKafkaMsg msg,MetaTableIndex metaIndex) {
		if(!init(msg, metaIndex))
			return;
		if (Operate.ADD.equals(msg.getOp())) {
			handleAddMetaIdx(msg, metaIndex);
		}
		else{
			handleRemoveMetaIdx(msg, metaIndex);
		}
	}
	
	protected ZkMetaIdxData handleRemoveMetaIdx(IdxKafkaMsg msg, MetaTableIndex metaIndex) {
		MetaFamilyIndex mfi=metaIndex.getTbIndexMap().get(msg.getTbName()).get(msg.getFamilyName());
		if(metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName())==null)
			metaIndex.getTbIndexNameMap().get(msg.getTbName()).put(msg.getFamilyName(), new MetaFamilyPosIndex());
		metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName()).getMapqulifier().remove(msg.getMetaIndex().getIdxName());
		for(StringList s:mfi.getMultiIndexMap().keySet()){
			MetaIndex mi=mfi.getMultiIndexMap().get(s);
			if(msg.getMetaIndex().getIdxName().equals(mi.getIdxName())){
				mfi.getMultiIndexMap().remove(s);
				break;
			}
		}
		ZkMetaIdxData zk=new ZkMetaIdxData();
		zk.setMfi(mfi);
		zk.setMfpi(metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName()));
		return zk;
	}
	
	protected ZkMetaIdxData handleAddMetaIdx(IdxKafkaMsg msg, MetaTableIndex metaIndex) {
		MetaFamilyIndex mfi=metaIndex.getTbIndexMap().get(msg.getTbName()).get(msg.getFamilyName());
		if(metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName())==null)
			metaIndex.getTbIndexNameMap().get(msg.getTbName()).put(msg.getFamilyName(), new MetaFamilyPosIndex());
		metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName()).getMapqulifier().put(msg.getMetaIndex().getIdxName(),msg.getColList());
		mfi.getMultiIndexMap().put(msg.getColList(), msg.getMetaIndex());
		ZkMetaIdxData zk=new ZkMetaIdxData();
		zk.setMfi(mfi);
		zk.setMfpi(metaIndex.getTbIndexNameMap().get(msg.getTbName()).get(msg.getFamilyName()));
		return zk;
	}
	
	protected boolean init(IdxKafkaMsg msg, MetaTableIndex metaIndex) {
		if(metaIndex==null||metaIndex.getTbIndexMap().get(msg.getTbName())==null)
			return false;
		MetaFamilyIndex mfi=metaIndex.getTbIndexMap().get(msg.getTbName()).get(msg.getFamilyName());
		if(mfi==null){
			mfi=new MetaFamilyIndex();
			metaIndex.getTbIndexMap().get(msg.getTbName()).put(msg.getFamilyName(), mfi);
		}
		return true;
	}
}
