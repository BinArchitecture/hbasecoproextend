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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;

public class MetaFamilyPosIndex implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2648843061712218609L;
	public int getDescpos() {
		return descpos;
	}
	public void setDescpos(int descpos) {
		this.descpos = descpos;
	}
	public Map<String, StringList> getMapqulifier() {
		return mapqulifier;
	}
	public void setMapqulifier(Map<String, StringList> mapqulifier) {
		this.mapqulifier = mapqulifier;
	}
	private int descpos;
	private Map<String,StringList> mapqulifier=new HashMap<String,StringList>();
}
