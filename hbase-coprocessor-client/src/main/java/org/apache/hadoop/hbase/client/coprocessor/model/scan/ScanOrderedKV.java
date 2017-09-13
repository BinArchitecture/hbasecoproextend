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

import org.apache.hadoop.hbase.client.coprocessor.model.MainHbaseCfPk;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class ScanOrderedKV implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8180639962986451454L;

	public ScanOrderedKV() {
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((op == null) ? 0 : op.hashCode());
		result = prime * result
				+ ((qulifier == null) ? 0 : qulifier.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScanOrderedKV other = (ScanOrderedKV) obj;
		if (op != other.op)
			return false;
		if (qulifier == null) {
			if (other.qulifier != null)
				return false;
		} else if (!qulifier.equals(other.qulifier))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public ScanOrderedKV(String qulifier, String value,CompareOp op) {
		this.qulifier = qulifier;
		this.value = value;
		this.op = op;
	}

	public CompareOp getOp() {
		return op;
	}

	public void setOp(CompareOp op) {
		this.op = op;
	}

	private CompareOp op;
	private String qulifier;
	private String value;

	public StringBuilder buildIdxPrefix(String sign){
		return new StringBuilder(qulifier).append(sign).append(value);
	}
	
	public void checkAndSetMainValue(MainHbaseCfPk main){
		if(this.qulifier.equals(main.getOidQulifierName()))
			this.value=main.buidFixedLengthNumber(this.value);
	}
	
	public String getQulifier() {
		return qulifier;
	}

	public void setQulifier(String qulifier) {
		this.qulifier = qulifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
