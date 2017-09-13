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

import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;

public class IdxKafkaMsg implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2959090716086436755L;
	private Operate op;
	private String tbName;
	private String familyName;
	private StringList colList;
	private MetaIndex metaIndex;

	public StringList getColList() {
		return colList;
	}

	public void setColList(StringList colList) {
		this.colList = colList;
	}

	public String getTbName() {
		return tbName;
	}

	public void setTbName(String tbName) {
		this.tbName = tbName;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public MetaIndex getMetaIndex() {
		return metaIndex;
	}

	public void setMetaIndex(MetaIndex metaIndex) {
		this.metaIndex = metaIndex;
	}

	public Operate getOp() {
		return op;
	}

	public void setOp(Operate op) {
		this.op = op;
	}

	public enum Operate {
		ADD, DROP,DROPFAMILY,ADDFAMILY,ADDIDXLOCK,RELEASEIDXLOCK
	}
}
