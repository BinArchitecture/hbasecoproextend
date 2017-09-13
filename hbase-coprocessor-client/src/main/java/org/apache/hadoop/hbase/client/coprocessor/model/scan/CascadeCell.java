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
package org.apache.hadoop.hbase.client.coprocessor.model.scan;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.FamilyValue;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSON;

public class CascadeCell implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3417528890526641562L;
	private byte[] row;
	private byte[] family;

	public Map<String, FamilyValue> getParentRKey() {
		return parentRKey;
	}

	public void setParentRKey(LinkedHashMap<String, FamilyValue> parentRKey) {
		this.parentRKey = parentRKey;
	}

	private LinkedHashMap<String, FamilyValue> parentRKey;

	public CascadeCell() {
	}

	@SuppressWarnings("deprecation")
	public CascadeCell build(List<Cell> cellList) {
		for (Cell c : cellList) {
			qulifyerValueMap.put(Bytes.toString(c.getQualifier()),
					Bytes.toString(c.getValue()));
		}
		return this;
	}

	public void buildCasCadeRowKey(String rowParent, int pos,
			Map<String,String> desc) {
		String[] ss = rowParent.split(Constants.SPLITTER);
		StringBuilder sb = new StringBuilder(ss[0]).append(Constants.SPLITTER)
				.append(ss[1]).append(Constants.SPLITTER);
		RowKeyComposition rkc = JSON.parseObject(desc.get(Bytes.toString(family)),
				RowKeyComposition.class);
		if (rkc == null)
			throw new IllegalStateException("family:" + family
					+ " need desc meta info");
		TreeSet<String> colKeysForRow = rkc.getFamilyColsForRowKey();
		for (String q : colKeysForRow) {
			String v = qulifyerValueMap.get(q);
			if (v == null)
				throw new IllegalStateException("Col Value Map must contain:"
						+ q);
			if(rkc.getMainHbaseCfPk().getOidQulifierName().equals(q))
				v=rkc.getMainHbaseCfPk().buidFixedLengthNumber(v);
			sb.append(q).append(Constants.QSPLITTER).append(v).append(Constants.SPLITTER);
		}
		if (parentRKey != null)
			for (String s : parentRKey.keySet()){
				FamilyValue fv=parentRKey.get(s);
				RowKeyComposition r = JSON.parseObject(desc.get(fv.getFamilyName()),
						RowKeyComposition.class);
				sb.append(s).append(Constants.QSPLITTER)
				.append(r.getMainHbaseCfPk().buidFixedLengthNumber(fv.getValue()))
						.append(Constants.SPLITTER);
			}
				
		sb.append(ss[pos]);
		this.row = sb.toString().getBytes();
	}

	public CascadeCell(byte[] row, byte[] family) {
		this.row = row;
		this.family = family;
	}

	private Map<String, String> qulifyerValueMap = new HashMap<String, String>();

	public byte[] getRow() {
		return row;
	}

	public void setRow(byte[] row) {
		this.row = row;
	}

	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public Map<String, String> getQulifyerValueMap() {
		return qulifyerValueMap;
	}

	public void setQulifyerValueMap(Map<String, String> qulifyerValueMap) {
		this.qulifyerValueMap = qulifyerValueMap;
	}
}
