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
package org.apache.hadoop.hbase.coprocessor.endpoint;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.groupby.HbaseGroupByCheckException;
import org.apache.hadoop.hbase.client.coprocessor.exception.groupby.HbaseGroupByConvertException;
import org.apache.hadoop.hbase.client.coprocessor.exception.groupby.HbaseGroupByUnKnowColumnException;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Avg;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Count;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.GroupBy;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Stg;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.util.RegionScanUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggregateGroupByRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggregateGroupByService;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete AggregateProtocol implementation. Its system level coprocessor
 * that computes the aggregate function at a region level.
 * {@link ColumnInterpreter} is used to interpret column value. This class is
 * parameterized with the following (these are the types with which the
 * {@link ColumnInterpreter} is parameterized, and for more description on
 * these, refer to {@link ColumnInterpreter}):
 */
@InterfaceAudience.Private
public class AggregateGroupByImplementation extends AggregateGroupByService
		implements CoprocessorService, Coprocessor {
	protected static final Log log = LogFactory
			.getLog(AggregateGroupByImplementation.class);
	private RegionCoprocessorEnvironment env;

	@Override
	public Service getService() {
		return this;
	}

	/**
	 * Stores a reference to the coprocessor environment provided by the
	 * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from
	 * the region where this coprocessor is loaded. Since this is a coprocessor
	 * endpoint, it always expects to be loaded on a table region, so always
	 * expects this to be an instance of {@link RegionCoprocessorEnvironment}.
	 * 
	 * @param env
	 *            the environment provided by the coprocessor host
	 * @throws IOException
	 *             if the provided environment is not an instance of
	 *             {@code RegionCoprocessorEnvironment}
	 */
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// nothing to do
	}

	@Override
	public void getMax(RpcController controller,
			AggregateGroupByRequest request, RpcCallback<AggregateResponse> done) {
		RegionScanner rs = null;
		AggregateResponse response = null;
		Map<String, BigDecimal> m = new HashMap<String, BigDecimal>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkColAndGroupBy(scan, request,true);
			if(request.getScanRangeCount()==0)
				buildMaxAggMap(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildMaxAggMap(request, m, scan,range);
				}
			}
			if (!m.isEmpty()) {
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Stg(m), -1));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		} 
		catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.info("Maximum from this region is "
				+ env.getRegion().getRegionInfo().getRegionNameAsString()
				+ ": " + m);
		done.run(response);
	}

	@SuppressWarnings("unchecked")
	private <T> void buildGroupScanMaxMin(AggregateGroupByRequest request,
			Map<String, T> m, Scan scan,String range,String type) throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			hasMoreRows = rs.next(results);
			if(results==null||results.isEmpty())
				continue;
			GroupBy gb;
			try {
				gb = convertGroupBy(request, results,true);
			} catch (Exception e2) {
				throw e2;
			}
			String classType=request.getAggTypeQulalifier().getClassType();
			if(classType==null)
				throw new HbaseGroupByCheckException("classType can not be null when do max or min operation");
			T t=m.get(gb.getGroupBy());
			if(classType.equals(String.class.getSimpleName())){
				try {
					if (t == null) {
						m.put(gb.getGroupBy(), (T) Bytes.toString(gb.getQulifierValue()));
						results.clear();
						continue;
					}
					String s=(String)t;
					if("max".equals(type))
					m.put(gb.getGroupBy(), s.compareTo(Bytes.toString(gb.getQulifierValue()))>=0?
							t:(T) Bytes.toString(gb.getQulifierValue()));
					else if("min".equals(type))
					m.put(gb.getGroupBy(), s.compareTo(Bytes.toString(gb.getQulifierValue()))<=0?
								t:(T) Bytes.toString(gb.getQulifierValue()));
				}catch(NullPointerException e1){
					throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
				}
			}
			else if(classType.equals(Double.class.getSimpleName())){
				Double ds=0d;
				try {
					ds = Double.valueOf(Bytes.toString(gb.getQulifierValue()));
				} catch (NumberFormatException e) {
					throw new HbaseGroupByConvertException(e.getMessage());
				}
				catch(NullPointerException e1){
					throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
				}
				if (t == null) {
					m.put(gb.getGroupBy(), (T)ds);
					results.clear();
					continue;
				}
				Double s=(Double)t;
				if("max".equals(type))
				m.put(gb.getGroupBy(), s>=ds?
						t:(T)ds);
				else if("min".equals(type))
					m.put(gb.getGroupBy(), s<=ds?
							t:(T)ds);
			}
			else if(classType.equals(Date.class.getSimpleName())){
				Date dd=null;
				try {
					dd=Constants.getSimpleDateFormat().parse(Bytes.toString(gb.getQulifierValue()));
				} catch (ParseException|NumberFormatException e1) {
					throw new HbaseGroupByConvertException(e1.getMessage());
				}
				catch(NullPointerException e1){
					throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
				}
				if (t == null) {
				    m.put(gb.getGroupBy(), (T)dd);
					results.clear();
					continue;
				}
				Date s=(Date)t;
				if("max".equals(type))
				m.put(gb.getGroupBy(), s.compareTo(dd)>=0?
							t:(T) dd);
				else if("min".equals(type))
					m.put(gb.getGroupBy(), s.compareTo(dd)<=0?
							t:(T) dd);
			}
			results.clear();
		} while (hasMoreRows);
	}

	private void buildGroupScanRange(Scan scan, String range) {
		if (!StringUtils.isBlank(range)) {
			scan.setStartRow(new StringBuilder(range)
					.append(Constants.REGTABLEHBASESTART).toString().getBytes());
			scan.setStopRow(new StringBuilder(range)
					.append(Constants.REGTABLEHBASESTOP).toString().getBytes());
		}
	}

	@Override
	public void getMin(RpcController controller,
			AggregateGroupByRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Map<String, BigDecimal> m = new HashMap<String, BigDecimal>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkColAndGroupBy(scan, request,true);
			if(request.getScanRangeCount()==0)
				buildMinAggMap(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildMinAggMap(request, m, scan,range);
				}
			}
			if (!m.isEmpty()) {
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Stg(m), -1));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		} 
		catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.info("Minimum from this region is "
				+ env.getRegion().getRegionInfo().getRegionNameAsString()
				+ ": " + m);
		done.run(response);

	}

	private <T> void buildMinAggMap(AggregateGroupByRequest request,
			Map<String, T> m, Scan scan,String range) throws IOException {
		buildGroupScanMaxMin(request,m,scan,range,"min");
	}
	
	private <T> void buildMaxAggMap(AggregateGroupByRequest request,
			Map<String, T> m, Scan scan,String range) throws IOException {
		buildGroupScanMaxMin(request,m,scan,range,"max");
	}

	@Override
	public void getSum(RpcController controller,
			AggregateGroupByRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Map<String, BigDecimal> m = new HashMap<String, BigDecimal>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkColAndGroupBy(scan, request,true);
			if(request.getScanRangeCount()==0)
				buildSumAgg(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildSumAgg(request, m, scan,range);
				}
			}
			if (!m.isEmpty()) {
				Map<String,Double> mp=buildSumDouble(m);
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Stg<Double>(mp), -1));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		} 
		catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.debug("Sum from this region is "
				+ env.getRegion().getRegionInfo().getRegionNameAsString()
				+ ": " + m);
		done.run(response);
	}

	private Map<String, Double> buildSumDouble(Map<String, BigDecimal> m) {
		Map<String, Double> map=new HashMap<String, Double>(m.size());
		for(String s:m.keySet()){
			map.put(s, m.get(s).doubleValue());
		}
		return map;
	}

	private void buildSumAgg(AggregateGroupByRequest request,
			Map<String, BigDecimal> m, Scan scan, String range) throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			hasMoreRows = rs.next(results);
			if(results.isEmpty())
				continue;
			GroupBy gb;
			try {
				gb = convertGroupBy(request, results,true);
			} catch (Exception e2) {
				throw e2;
			}
			BigDecimal lb = m.get(gb.getGroupBy());
			if (lb == null) {
				lb = BigDecimal.ZERO;
			}
			try {
				lb=lb.add(BigDecimal.valueOf(Double.parseDouble(Bytes.toString(gb.getQulifierValue()))));
			} catch (NumberFormatException e) {
				throw new HbaseGroupByConvertException(e.getMessage());
			}
			catch(NullPointerException e1){
				throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
			}
			m.put(gb.getGroupBy(), lb);
			results.clear();
		} while (hasMoreRows);
	}

	@Override
	public void getRowNum(RpcController controller,
			AggregateGroupByRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Map<String, Integer> m = new HashMap<String, Integer>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkColAndGroupBy(scan, request,false);
			if(request.getScanRangeCount()==0)
				buildRowCountAgg(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildRowCountAgg(request, m, scan,range);
				}
			}
			if (!m.isEmpty()) {
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Count(m), -1));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		} 
		catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.info("Row counter from this region is "
				+ env.getRegion().getRegionInfo().getRegionNameAsString()
				+ ": " + m);
		done.run(response);
	}

	private void buildRowCountAgg(AggregateGroupByRequest request, 
			Map<String, Integer> m, Scan scan,String range)
			throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			hasMoreRows = rs.next(results);
			if(results.isEmpty())
				continue;
			GroupBy gb;
			try {
				gb = convertGroupBy(request, results,false);
			} catch (Exception e) {
				throw e;
			}
			Integer lb = m.get(gb.getGroupBy());
			if (lb == null) {
				lb = 0;
			}
			m.put(gb.getGroupBy(), ++lb);
			results.clear();
		} while (hasMoreRows);
	}

	@Override
	public void getAvg(RpcController controller,
			AggregateGroupByRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Map<String, List<byte[]>> m = new HashMap<String, List<byte[]>>();
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkColAndGroupBy(scan, request,true);
			if(request.getScanRangeCount()==0)
				buildAvgAgg(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildAvgAgg(request, m, scan,range);
				}
			}
			if (!m.isEmpty()) {
				Map<String, Std> mm = new HashMap<String, Std>(m.size());
				for (String k : m.keySet()) {
					List<byte[]> l = m.get(k);
					BigDecimal bb = BigDecimal.ZERO;
					for (byte[] b : l) {
						try {
							bb=bb.add(BigDecimal.valueOf(Double.parseDouble(Bytes.toString(b))));
						} catch (NumberFormatException e) {
							throw new HbaseGroupByConvertException(e.getMessage());
						}
						catch(NullPointerException e1){
							throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
						}
					}
					mm.put(k, new Std(bb, l.size()));
				}
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Avg(mm), -1));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		} 
		catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		done.run(response);
	}

	private void buildAvgAgg(AggregateGroupByRequest request,
			Map<String, List<byte[]>> m, Scan scan,String range) throws IOException,LinkageError {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			results.clear();
			hasMoreRows = rs.next(results);
			if(results.isEmpty())
				continue;
			GroupBy gb;
			try {
				gb = convertGroupBy(request, results,true);
			} catch (Exception e) {
				throw e;
			}
			List<byte[]> lb = m.get(gb.getGroupBy());
			if (lb == null) {
				lb = new ArrayList<byte[]>();
			}
			try {
				lb.add(gb.getQulifierValue());
			}
			catch(NullPointerException e1){
				throw new HbaseGroupByUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
			}
			m.put(gb.getGroupBy(), lb);
		} while (hasMoreRows);
	}

	private void checkColAndGroupBy(Scan scan, AggregateGroupByRequest request,boolean check) throws IOException {
		byte[] colFamily = scan.getFamilies()[0];
		if (check) {
			NavigableSet<byte[]> qualifiers = scan.getFamilyMap()
					.get(colFamily);
			if (qualifiers == null
					|| !qualifiers.contains(request.getAggTypeQulalifier()
							.getQulifier().getBytes())) {
				throw new HbaseGroupByCheckException("scan does not contain qulifier");
			}
		}
		for(String qulifierGroupBy:request.getGroupbyList()){
			scan.addColumn(colFamily, qulifierGroupBy.getBytes());
		}
	}

	@SuppressWarnings("deprecation")
	private GroupBy convertGroupBy(AggregateGroupByRequest request,
			List<Cell> results,boolean needQualiFier) {
		GroupBy gb = new GroupBy();
		StringBuilder grpb = new StringBuilder("");
		if (!results.isEmpty())
			for (Cell c : results) {
				if (needQualiFier) {
					if (Bytes.toString(c.getQualifier()).equals(
							request.getAggTypeQulalifier().getQulifier())) {
						gb.setQulifierValue(c.getValue());
					}
				}
				if (request.getGroupbyList().contains(
						Bytes.toString(c.getQualifier()))) {
					grpb.append(Bytes.toString(c.getValue())).append(",");
				}
			}
		if(grpb.length()>1)
		gb.setGroupBy(grpb.substring(0,grpb.length()-1));
		else{
			StringBuilder groupByList=new StringBuilder("");
			for(String s:request.getGroupbyList())
				groupByList.append(s).append(",");
			throw new HbaseGroupByUnKnowColumnException(groupByList.substring(0,groupByList.length()-1)+" does not exist!");
		}
		return gb;
	}
}
