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
package org.apache.hadoop.hbase.client.coprocessor.model.idx;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.coprocessor.model.MainHbaseCfPk;

public class RowKeyComposition implements Serializable{
	public HbaseDataType getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(HbaseDataType orderBy) {
		this.orderBy = orderBy;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 955512431436064740L;
	private MainHbaseCfPk mainHbaseCfPk;
	private TreeSet<String> familyColsForRowKey;
	private LinkedHashSet<String> familyColsNeedIdx;
	private HbaseDataType orderBy;
	public RowKeyComposition(){}
	public RowKeyComposition(MainHbaseCfPk mainHbaseCfPk,TreeSet<String> familyColsForRowKey,HbaseDataType orderBy){
		this.mainHbaseCfPk=mainHbaseCfPk;
		this.familyColsForRowKey=familyColsForRowKey;
		this.orderBy=orderBy;
		checkSet();
	}
	public MainHbaseCfPk getMainHbaseCfPk() {
		return mainHbaseCfPk;
	}
	public void setMainHbaseCfPk(MainHbaseCfPk mainHbaseCfPk) {
		this.mainHbaseCfPk = mainHbaseCfPk;
	}
	private void checkSet() {
		for(String familyColsForRow:familyColsForRowKey){
			if(familyColsForRow.equals(mainHbaseCfPk.getOidQulifierName()))
				return;
		}
		throw new IllegalStateException("familyColsForRowKey must contain oidQulifierName!"); 
	}
	public TreeSet<String> getFamilyColsForRowKey() {
		return familyColsForRowKey;
	}
	public void setFamilyColsForRowKey(TreeSet<String> familyColsForRowKey) {
		this.familyColsForRowKey = familyColsForRowKey;
	}
	
	public int buildOidPos(){
		if(familyColsForRowKey==null||mainHbaseCfPk.getOidQulifierName()==null)
			return -1;
		int i=0;
		for(String s:familyColsForRowKey){
			if(s.equals(mainHbaseCfPk.getOidQulifierName())){
				return i;
			}
			i++;
		}
		return -1;
	}
	public LinkedHashSet<String> getFamilyColsNeedIdx() {
		return familyColsNeedIdx;
	}
	public void setFamilyColsNeedIdx(LinkedHashSet<String> familyColsNeedIdx) {
		this.familyColsNeedIdx = familyColsNeedIdx;
	}
}
