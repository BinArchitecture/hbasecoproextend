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
import java.text.NumberFormat;

import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

public class MainHbaseCfPk implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2823233488071915506L;
	private String oidQulifierName;
	private int fixedLength;

	public MainHbaseCfPk() {
	}

	public MainHbaseCfPk(String oidQulifierName, int fixedLength) {
		this.oidQulifierName = oidQulifierName;
		this.fixedLength = fixedLength;
	}

	public String buidFixedLengthNumber(String pkValueStr) {
		try {
			Long pkValue=Long.parseLong(pkValueStr);
			NumberFormat nf = NumberFormat.getInstance();
			nf.setGroupingUsed(false);
			nf.setMaximumIntegerDigits(fixedLength);
			nf.setMinimumIntegerDigits(fixedLength);
			return nf.format(pkValue);
		} catch (NumberFormatException e) {
			return HbaseUtil.addZeroForNum(pkValueStr, fixedLength);
		}
	}

	public String getOidQulifierName() {
		return oidQulifierName;
	}

	public void setOidQulifierName(String oidQulifierName) {
		this.oidQulifierName = oidQulifierName;
	}

	public int getFixedLength() {
		return fixedLength;
	}

	public void setFixedLength(int fixedLength) {
		this.fixedLength = fixedLength;
	}
}