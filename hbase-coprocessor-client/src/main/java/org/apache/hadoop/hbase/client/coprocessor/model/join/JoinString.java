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
package org.apache.hadoop.hbase.client.coprocessor.model.join;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

public class JoinString implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3559101014429688341L;
	private String familyName;
	private String joinPoint;
	private List<String> qualColList;
	private Scan scan=new Scan();
	public Scan getScan() {
		return scan;
	}
	public void setScan(Scan scan) {
		this.scan = scan;
	}
	public JoinString(String familyName,String joinPoint,Scan scan,List<String> qualColList){
		this.familyName=familyName;
		this.joinPoint=joinPoint;
		this.scan = scan;
		this.qualColList = qualColList;
	}
	public String getFamilyName() {
		return familyName;
	}
	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}
	public String getJoinPoint() {
		return joinPoint;
	}
	public void setJoinPoint(String joinPoint) {
		this.joinPoint = joinPoint;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((familyName == null) ? 0 : familyName.hashCode());
		result = prime * result
				+ ((joinPoint == null) ? 0 : joinPoint.hashCode());
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
		JoinString other = (JoinString) obj;
		if (familyName == null) {
			if (other.familyName != null)
				return false;
		} else if (!familyName.equals(other.familyName))
			return false;
		if (joinPoint == null) {
			if (other.joinPoint != null)
				return false;
		} else if (!joinPoint.equals(other.joinPoint))
			return false;
		return true;
	}
	public List<String> getQualColList() {
		return qualColList;
	}
	public void setQualColList(List<String> qualColList) {
		this.qualColList = qualColList;
	}
}
