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
package org.apache.hadoop.hbase.client.coprocessor.model;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

public class EsIdxHbaseType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6971332277688176406L;
	private String columnName;
	private String idxValue;
	private String prefix;
	private String familyName;
	private String TbName;
	private String rowKey;
	private String mainColumn;

	public String buildEsId(){
		Map<String,String> conMap=HbaseUtil.buildConMapByRowKey(rowKey);
		String mainId=conMap.get(mainColumn);
		return new StringBuilder(prefix).append(Constants.SPLITTER).append(familyName)
				.append(Constants.SPLITTER).append(columnName).append(Constants.SPLITTER).append(mainId).
				toString();
	}
	
	public enum Operation {
		Insert,Update,Delete,ADDTB,ADDCF,DROPCF,DROPTB
	}
	private Operation op; 
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getIdxValue() {
		return idxValue;
	}

	public void setIdxValue(String idxValue) {
		this.idxValue = idxValue;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public String getTbName() {
		return TbName;
	}

	public void setTbName(String tbName) {
		TbName = tbName;
	}

	public Operation getOp() {
		return op;
	}

	public void setOp(Operation op) {
		this.op = op;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getMainColumn() {
		return mainColumn;
	}

	public void setMainColumn(String mainColumn) {
		this.mainColumn = mainColumn;
	}
}