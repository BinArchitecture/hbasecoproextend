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
package org.apache.hadoop.hbase.coprocessor.observer.handler.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverUpdateHandler;


public class IndexNonOrderByRegionObserverHandler extends IndexRegionObserverUpdateHandler{
	protected static final Log logger = LogFactory
			.getLog(IndexNonOrderByRegionObserverHandler.class);
	private static IndexNonOrderByRegionObserverHandler instance=new IndexNonOrderByRegionObserverHandler();
	private IndexNonOrderByRegionObserverHandler(){}
	public static IndexNonOrderByRegionObserverHandler getInstance(){
		return instance;
	}
	@Override
	public String buildUpdateRowIdxPut(String row,List<Cell> l,String tbName, String familyName,Map<StringList, MetaIndex> multiIndexMap){
		StringBuilder sb=new StringBuilder("");
		String[] pre1=row.split(Constants.SPLITTER);
		String prefix=sb.append(pre1[0]).append(Constants.SPLITTER).append(pre1[1])
				.append(Constants.SPLITTER).append(pre1[2]).append(Constants.SPLITTER).toString();
		return build(row,prefix,l,pre1);
	}
	private String build(String row,String prefix,List<Cell> l,String[] pre1) {
		StringBuilder sb=new StringBuilder(prefix);
		CascadeCell cac=new CascadeCell().build(l);
		for(String idxQ:pre1){
			if(idxQ.contains(Constants.QSPLITTER)){
				String[] ii=idxQ.split(Constants.QSPLITTER);
				if(cac.getQulifyerValueMap().containsKey(ii[0])){
					sb.append(ii[0]).append(Constants.QSPLITTER).
					append(cac.getQulifyerValueMap().get(ii[0]));
				}
				else{
					sb.append(idxQ);
				}
				sb.append(Constants.SPLITTER);
			}
		}
		return sb.substring(0, sb.length()-1);
	}
	
	public String buildUpdateRowPut(String row,List<Cell> l,String familyName,HTableDescriptor desc){
		if(isNeedToUpdateRowkey(l, familyName, desc).isMark()){
			StringBuilder sb=new StringBuilder("");
			String[] pre1=row.split(Constants.SPLITTER);
			String prefix=sb.append(pre1[0]).append(Constants.SPLITTER).append(pre1[1])
					.append(Constants.SPLITTER).toString();
			return build(row,prefix,l,pre1);
		}
		return null;
	}
}