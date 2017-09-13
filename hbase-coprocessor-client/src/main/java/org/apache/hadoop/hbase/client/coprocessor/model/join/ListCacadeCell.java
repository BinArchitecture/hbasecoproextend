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
import java.util.List;
import java.util.Map;

public class ListCacadeCell implements Serializable{
	private static final long serialVersionUID = 694848860473847678L;
	private	List<Map<JoinRelationShip,String>> cascadeCellList=new ArrayList<Map<JoinRelationShip,String>>();
	private String jedisPrefixNo;
	public String getJedisPrefixNo() {
		return jedisPrefixNo;
	}
	public void setJedisPrefixNo(String jedisPrefixNo) {
		this.jedisPrefixNo = jedisPrefixNo;
	}
	public List<Map<JoinRelationShip, String>> getCascadeCellList() {
		return cascadeCellList;
	}
	public void setCascadeCellList(
			List<Map<JoinRelationShip, String>> cascadeCellList) {
		this.cascadeCellList = cascadeCellList;
	}
}