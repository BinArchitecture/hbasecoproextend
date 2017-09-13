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
package org.apache.hadoop.hbase.coprocessor.observer.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.HbaseDataModel;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.coprocessor.observer.handler.impl.IndexNonOrderByRegionObserverHandler;
import org.apache.hadoop.hbase.coprocessor.observer.handler.impl.IndexOrderByRegionObserverHandler;

import com.alibaba.fastjson.JSON;

public class IndexRegionObserverUpdateHandler {
	protected static final Log logger = LogFactory
			.getLog(IndexRegionObserverUpdateHandler.class);
	private static IndexRegionObserverUpdateHandler instance=new IndexRegionObserverUpdateHandler();
	public IndexRegionObserverUpdateHandler(){
		this.mapInstance=new HashMap<Boolean,IndexRegionObserverUpdateHandler>(2);
		mapInstance.put(true, IndexOrderByRegionObserverHandler.getInstance());
		mapInstance.put(false, IndexNonOrderByRegionObserverHandler.getInstance());
	}
	public static IndexRegionObserverUpdateHandler getInstance(){
		return instance;
	}
	private Map<Boolean,IndexRegionObserverUpdateHandler> mapInstance;
	public HbaseDataModel isNeedToUpdateRowkey(List<Cell> l, String familyName,
			HTableDescriptor desc) {
		String sv=desc.getValue(familyName);
		if(!StringUtils.isBlank(sv)){
			RowKeyComposition rkc=JSON.parseObject(desc.getValue(familyName),RowKeyComposition.class);
			CascadeCell cc=new CascadeCell().build(l);
			boolean boo=false;
			for(String s:rkc.getFamilyColsForRowKey()){
				if(cc.getQulifyerValueMap().containsKey(s)){
					boo=true;
					break;
				}
			}
		   return new HbaseDataModel(boo,rkc.getOrderBy());
		}
		return new HbaseDataModel(false,null);
	}
	public String buildUpdateRowIdxPut(String row,List<Cell> l,String tbName, String familyName,Map<StringList, MetaIndex> multiIndexMap){
		return mapInstance.get(row.contains(Constants.QLIFIERSPLITTER)).
				buildUpdateRowIdxPut(row, l, tbName, familyName, multiIndexMap);
	}
	
	public String buildUpdateRowPut(String row,List<Cell> l,String familyName,HTableDescriptor desc){
		return mapInstance.get(row.contains(Constants.QLIFIERSPLITTER)).
				buildUpdateRowPut(row, l,familyName, desc);
	}
}