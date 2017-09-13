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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetaTableIndex implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = 3190365862780741626L;
private Map<String,Map<String,MetaFamilyIndex>> tbIndexMap=new ConcurrentHashMap<String,Map<String,MetaFamilyIndex>>();
private Map<String,Map<String,MetaFamilyPosIndex>> tbIndexNameMap=new ConcurrentHashMap<String,Map<String,MetaFamilyPosIndex>>();
public Map<String, Map<String, MetaFamilyPosIndex>> getTbIndexNameMap() {
	return tbIndexNameMap;
}
public void setTbIndexNameMap(
		Map<String, Map<String, MetaFamilyPosIndex>> tbIndexNameMap) {
	this.tbIndexNameMap = tbIndexNameMap;
}
public Map<String, Map<String, MetaFamilyIndex>> getTbIndexMap() {
	return tbIndexMap;
}
public void setTbIndexMap(Map<String, Map<String, MetaFamilyIndex>> tbIndexMap) {
	this.tbIndexMap = tbIndexMap;
}
}