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
package org.apache.hadoop.hbase.client.coprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;

public class MultiIndexFetcher implements MetaIndexFetcher{
	private static MultiIndexFetcher fetcher=new MultiIndexFetcher();
	private MultiIndexFetcher(){}
	public static MultiIndexFetcher getInstance(){
		return fetcher;
	}
	@Override
	public MetaIndex genMetaIndex(Set<String> qulifierList,List<HbaseDataType> lhd,MetaFamilyIndex mlm) {
		if(mlm==null)
			return null;
		Map<StringList, MetaIndex> multiIndexMap = mlm.getMultiIndexMap();
		if(multiIndexMap==null)
			return null;
		Map<String,MetaIndex> tree=new TreeMap<String,MetaIndex>();
		List<String> qq=new ArrayList<String>(qulifierList.size());
		for(String q:qulifierList){
			qq.add(q);
		}
		for(StringList s:multiIndexMap.keySet()){
			if(s.contain(qq.toArray(new String[0]))){
				if(lhd==null||s.contain(lhd)){
					MetaIndex mmi =multiIndexMap.get(s);
					tree.put(mmi.getIdxName(), mmi);
				}
			}
		}
		return tree.isEmpty()?null:tree.values().iterator().next();
	}
}