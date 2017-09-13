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
package org.apache.hadoop.hbase.coprocessor.observer.handler.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.HbaseDataModel;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverUpdateHandler;

public class IndexOrderByRegionObserverHandler extends
		IndexRegionObserverUpdateHandler {
	protected static final Log logger = LogFactory
			.getLog(IndexOrderByRegionObserverHandler.class);
	private static IndexOrderByRegionObserverHandler instance = new IndexOrderByRegionObserverHandler();

	private IndexOrderByRegionObserverHandler() {
	}

	public static IndexOrderByRegionObserverHandler getInstance() {
		return instance;
	}

	@Override
	public String buildUpdateRowIdxPut(String row, List<Cell> l, String tbName,
			String familyName,Map<StringList, MetaIndex> multiIndexMap) {
		String[] idxRowArray = row.split(Constants.QLIFIERSPLITTER, 2);
		StringBuilder sb = new StringBuilder("");
		String[] pre1 = idxRowArray[0].split(Constants.SPLITTER);
		String[] pre2 = idxRowArray[1].split(Constants.SPLITTER);
		sb.append(pre1[0]).append(Constants.SPLITTER).append(pre1[1])
				.append(Constants.SPLITTER).append(pre1[2])
				.append(Constants.SPLITTER);
		String idxName = pre1[2];
		StringList idxList = buildStringList(row, tbName, familyName,
				multiIndexMap, idxName);
		if (idxList == null)
			return row;
		CascadeCell cac = new CascadeCell().build(l);
		BlockingQueue<String> ll = new ArrayBlockingQueue<String>(idxList
				.getHbasedataList().size());
		for (String s : pre1) {
			if (s.contains(Constants.QSPLITTERORDERBY)) {
				for (String x : s.split(Constants.QSPLITTERORDERBY)) {
					ll.offer(x);
				}
			}
		}
		for (HbaseDataType hbd : idxList.getHbasedataList()) {
			String sm = "";
			try {
				sm = ll.take();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
			if (cac.getQulifyerValueMap().containsKey(hbd.getQulifier())) {
				String v = cac.getQulifyerValueMap().get(hbd.getQulifier());
				String vv = hbd.build(v);
				sb.append(vv).append(Constants.QSPLITTERORDERBY);
			} else {
				sb.append(sm).append(Constants.QSPLITTERORDERBY);
			}
		}
		sb = new StringBuilder(sb.substring(0, sb.length() - 1))
				.append(Constants.QLIFIERSPLITTER);
		int j = 0;
		for (String idxQ : pre2) {
			String[] ii = idxQ.split(Constants.QSPLITTER);
			if (cac.getQulifyerValueMap().containsKey(ii[0])) {
				sb.append(ii[0]).append(Constants.QSPLITTER)
						.append(cac.getQulifyerValueMap().get(ii[0]));
			} else {
				sb.append(idxQ);
			}
			if (j++ < pre2.length - 1)
				sb.append(Constants.SPLITTER);
		}
		return sb.toString();
	}

	public StringList buildStringList(String row, String tbName,
			String familyName, Map<StringList, MetaIndex> multiIndexMap, String idxName) {
		StringList idxList = null;
		try {
			for (StringList il : multiIndexMap.keySet()) {
				MetaIndex mi = multiIndexMap.get(il);
				if (mi.getIdxName().equals(idxName)) {
					idxList = il;
					break;
				}
			}
		} catch (Exception e1) {
			logger.error(e1.getMessage(), e1);
			return null;
		}
		return idxList;
	}

	public String buildUpdateRowPut(String row, List<Cell> l,
			String familyName, HTableDescriptor desc) {
		HbaseDataModel hdm = isNeedToUpdateRowkey(l, familyName, desc);
		if (hdm.isMark()) {
			String[] idxRowArray = row.split(Constants.QLIFIERSPLITTER, 2);
			StringBuilder sb = new StringBuilder("");
			String[] pre1 = idxRowArray[0].split(Constants.SPLITTER);
			String[] pre2 = idxRowArray[1].split(Constants.SPLITTER);
			sb.append(pre1[0]).append(Constants.SPLITTER).append(pre1[1])
					.append(Constants.SPLITTER);
			CascadeCell cac = new CascadeCell().build(l);
			String orderBy = buildOrderByRow(hdm.getHdt(), cac);
			sb.append(orderBy).append(Constants.QLIFIERSPLITTER);
			int j = 0;
			for (String idxQ : pre2) {
				String[] ii = idxQ.split(Constants.QSPLITTER);
				if (cac.getQulifyerValueMap().containsKey(ii[0])) {
					sb.append(ii[0]).append(Constants.QSPLITTER)
							.append(cac.getQulifyerValueMap().get(ii[0]));
				} else {
					sb.append(idxQ);
				}
				if (j++ < pre2.length - 1)
					sb.append(Constants.SPLITTER);
			}
			return sb.toString();
		}
		return null;
	}

	private String buildOrderByRow(HbaseDataType hdt, CascadeCell cac) {
		if (hdt == null)
			return "";
		if (cac.getQulifyerValueMap().containsKey(hdt.getQulifier())) {
			String v = hdt.build(cac.getQulifyerValueMap().get(
					hdt.getQulifier()));
			if (v != null)
				return v;
		}
		return "";
	}
}