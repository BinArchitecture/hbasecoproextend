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
import java.util.List;
import java.util.Map;

public class ScanCascadeResult implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = 4427971635471955823L;
public CascadeCell getSource() {
		return source;
	}
	public void setSource(CascadeCell source) {
		this.source = source;
	}
	public Map<String, List<ScanCascadeResult>> getCascadeMap() {
		return cascadeMap;
	}
	public void setCascadeMap(Map<String, List<ScanCascadeResult>> cascadeMap) {
		this.cascadeMap = cascadeMap;
	}
private CascadeCell source=new CascadeCell();
private Map<String,List<ScanCascadeResult>> cascadeMap=new HashMap<String,List<ScanCascadeResult>>();
}