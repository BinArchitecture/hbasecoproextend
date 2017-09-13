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
import java.util.HashMap;
import java.util.Map;

public class JoinRelationShip implements Serializable,Comparable<JoinRelationShip>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 879231497471985645L;
	private String tableName;
	public void setRelationShipMap(
			Map<JoinRelationShip, JoinRelationShip> relationShipMap) {
		this.relationShipMap = relationShipMap;
	}
	private String familyName;
	private String joinPoint;
	private Map<JoinRelationShip,JoinRelationShip> relationShipMap=new HashMap<JoinRelationShip,JoinRelationShip>(1);
	public Map<JoinRelationShip, JoinRelationShip> getRelationShipMap() {
		return relationShipMap;
	}
	public JoinRelationShip(){}
	public JoinRelationShip(String tableName,String familyName,String joinPoint){
		this.tableName=tableName;
		this.familyName=familyName;
		this.joinPoint=joinPoint;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((familyName == null) ? 0 : familyName.hashCode());
		result = prime * result
				+ ((joinPoint == null) ? 0 : joinPoint.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
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
		JoinRelationShip other = (JoinRelationShip) obj;
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
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
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
	public int compareTo(JoinRelationShip q) {
		return tableName.compareTo(q.tableName)|
				familyName.compareTo(q.familyName)|
				joinPoint.compareTo(q.joinPoint);
	}
}
