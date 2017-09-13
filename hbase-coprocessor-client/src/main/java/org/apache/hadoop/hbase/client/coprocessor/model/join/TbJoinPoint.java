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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;

public class TbJoinPoint implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1098241475151878770L;
	private String tableName;
	private String familyName;
	private Scan scan=new Scan();
	private String joinPoint;
	private List<String> qualColList;

	public TbJoinPoint() {
	}

	public TbJoinPoint(String tableName, String familyName, String joinPoint) {
		this.tableName = tableName;
		this.familyName = familyName;
		this.joinPoint = joinPoint;
	}

	private Map<String, TbJoinPoint> joinPointMap = new HashMap<String, TbJoinPoint>(1);

	public MapJoinRelationShip buildRelationShip(){
		MapJoinRelationShip ms=new MapJoinRelationShip();
		ms.setMapJoin(buildRelationShipMap());
		return ms;
	}
	
	private Map<JoinRelationShip,JoinRelationShip> buildRelationShipMap(){
		Map<JoinRelationShip,JoinRelationShip> m=new HashMap<JoinRelationShip,JoinRelationShip>(1);
		for(String s:joinPointMap.keySet()){
			TbJoinPoint tp=joinPointMap.get(s);
			Map<JoinRelationShip,JoinRelationShip> mm=tp.buildRelationShipMap();
			JoinRelationShip jsp=new JoinRelationShip(tp.getTableName(),tp.getFamilyName(),tp.getJoinPoint());
			m.put(new JoinRelationShip(tableName,familyName,s),jsp);
			if(!mm.isEmpty())
				jsp.getRelationShipMap().putAll(mm);
		}
		return m;
	}
	
	public Map<String, List<JoinString>> buildMap(String tableName) {
		Map<String, List<JoinString>> map = build();
		map.remove(tableName);
		return map;
	}

	private Map<String, List<JoinString>> build() {
		Map<String, List<JoinString>> mjoinPointMap = new HashMap<String, List<JoinString>>();
		List<JoinString> ll = new ArrayList<JoinString>();
		ll.add(new JoinString(familyName, joinPoint, scan, qualColList));
		mjoinPointMap.put(tableName, ll);
		if (joinPointMap.isEmpty())
			return mjoinPointMap;
		for (TbJoinPoint tjp : joinPointMap.values()) {
			Map<String, List<JoinString>> ttjMap = tjp.build();
			for (String s : ttjMap.keySet()) {
				List<JoinString> l = mjoinPointMap.get(s);
				if (l == null) {
					l = new ArrayList<JoinString>();
				}
				l.addAll(ttjMap.get(s));
				mjoinPointMap.put(s, l);
			}
		}
		return mjoinPointMap;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getJoinPoint() {
		return joinPoint;
	}

	public void setJoinPoint(String joinPoint) {
		this.joinPoint = joinPoint;
	}

	public Scan getScan() {
		return scan;
	}

	public void setScan(Scan scan) {
		this.scan = scan;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public Map<String, TbJoinPoint> getJoinPointMap() {
		return joinPointMap;
	}

	public void setJoinPointMap(Map<String, TbJoinPoint> joinPointMap) {
		this.joinPointMap = joinPointMap;
	}

	public List<String> getQualColList() {
		return qualColList;
	}

	public void setQualColList(List<String> qualColList) {
		this.qualColList = qualColList;
	}
}