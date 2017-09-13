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
import java.util.List;
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
import org.apache.hadoop.hbase.client.coprocessor.exception.agg.HbaseAggCheckException;
import org.apache.hadoop.hbase.client.coprocessor.exception.agg.HbaseAggConvertException;
import org.apache.hadoop.hbase.client.coprocessor.exception.agg.HbaseAggUnKnowColumnException;
import org.apache.hadoop.hbase.client.coprocessor.model.agg.Sagg;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.util.RegionScanUtil;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateStgProtos.AggregateStgRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateStgProtos.AggregateStgService;
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
public class AggregateStgImplementation extends AggregateStgService
		implements CoprocessorService, Coprocessor {
	protected static final Log log = LogFactory
			.getLog(AggregateStgImplementation.class);
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
	public void getMax(RpcController controller, AggregateStgRequest request,
			RpcCallback<AggregateResponse> done) {
		RegionScanner rs = null;
		AggregateResponse response = null;
		Sagg m = new Sagg(null);
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkCol(scan, request);
			if(request.getScanRangeCount()==0)
				buildMaxAggMap(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildMaxAggMap(request, m, scan,range);
				}
			}
			if (m.getT()!=null) {
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(m, 4096));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		}catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		} finally {
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

	private void buildMaxAggMap(AggregateStgRequest request, Sagg m, Scan scan, String range) throws IOException {
		buildScanMaxMin(request,m,scan,range,"max");	
		
	}

	@SuppressWarnings("unchecked")
	private void buildScanMaxMin(AggregateStgRequest request, Sagg m, Scan scan, String range, String type)
			throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs = RegionScanUtil.buildIdxScan(scan, env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			hasMoreRows = rs.next(results);
			if (results == null || results.isEmpty())
				continue;
			byte[] b = null;
			for (Cell c : results) {
				if (Bytes.toString(c.getQualifier()).equals(request.getAggTypeQulalifier().getQulifier())) {
					b = c.getValue();
					break;
				}
			}
			String classType = request.getAggTypeQulalifier().getClassType();
			if (classType == null)
				throw new HbaseAggCheckException("classType can not be null when do max or min operation");
			Object t = m.getT();
			if (b == null)
				throw new HbaseAggUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
			if (classType.equals(String.class.getSimpleName())) {
				String bs = Bytes.toString(b);
				if (t == null) {
					m.setT(bs);
					results.clear();
					continue;
				}
				String s = (String) t;
				if ("max".equals(type))
					m.setT(bs.compareTo(s) >= 0 ? bs : s);
				else if ("min".equals(type))
					m.setT(bs.compareTo(s) <=0 ? bs : s);
			} else if (classType.equals(Double.class.getSimpleName())) {
				BigDecimal bs = BigDecimal.ZERO;
				try {
					bs = BigDecimal.valueOf(Double.valueOf(Bytes.toString(b)));
				} catch (NumberFormatException e) {
					throw new HbaseAggConvertException(e.getMessage());
				}
				if (t == null) {
					m.setT(bs);
					results.clear();
					continue;
				}
				BigDecimal s = (BigDecimal) t;
				if ("max".equals(type))
					m.setT(bs.compareTo(s) >= 0 ? bs : s);
				else if ("min".equals(type))
					m.setT(bs.compareTo(s) <= 0 ? bs : s);
			} else if (classType.equals(Date.class.getSimpleName())) {
				Date dd = null;
				try {
					dd = Constants.getSimpleDateFormat().parse(Bytes.toString(b));
				} catch (ParseException|NumberFormatException e1) {
					throw new HbaseAggConvertException(e1.getMessage());
				}
				if (t == null) {
					m.setT(dd);
					results.clear();
					continue;
				}
				Date s = (Date) t;
				if ("max".equals(type))
					m.setT(dd.compareTo(s) >= 0 ? dd : s);
				else if ("min".equals(type))
					m.setT(dd.compareTo(s) <=0 ? dd : s);
			}
			results.clear();
		} while (hasMoreRows);
	}

	@Override
	public void getMin(RpcController controller, AggregateStgRequest request,
			RpcCallback<AggregateResponse> done) {
		RegionScanner rs = null;
		AggregateResponse response = null;
		Sagg m = new Sagg(null);
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkCol(scan, request);
			if(request.getScanRangeCount()==0)
				buildMinAggMap(request, m, scan,null);
			else{
				for(String range:request.getScanRangeList()){
					buildMinAggMap(request, m, scan,range);
				}
			}
			if (m.getT()!=null) {
				ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(m, 4096));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
			
		}catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		} finally {
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

	private void buildMinAggMap(AggregateStgRequest request, Sagg m, Scan scan, String range) throws IOException {
		buildScanMaxMin(request,m,scan,range,"min");		
	}

	@Override
	public void getSum(RpcController controller, AggregateStgRequest request,
			RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		BigDecimal m =BigDecimal.ZERO;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkCol(scan, request);
			if (request.getScanRangeCount() == 0)
				m=buildSumAgg(request, scan, null);
			else {
				for (String range : request.getScanRangeList()) {
					m=m.add(buildSumAgg(request, scan, range));
				}
			}
			ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Sagg<BigDecimal>(m), 4096));
			AggregateResponse.Builder pair = AggregateResponse.newBuilder();
			pair.addFirstPart(first);
			response = pair.build();

		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		}catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.debug("Sum from this region is " + env.getRegion().getRegionInfo().getRegionNameAsString() + ": " + m);
		done.run(response);
		
	}

	@Override
	public void getRowNum(RpcController controller,
			AggregateStgRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Integer rowNum = new Integer(0);
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
//			checkCol(scan, request);
			if (request.getScanRangeCount() == 0)
				rowNum=buildRowCountAgg(request, scan, null);
			else {
				for (String range : request.getScanRangeList()) {
					rowNum+=buildRowCountAgg(request, scan, range);
				}
			}
			ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(new Sagg<Integer>(rowNum), 4096));
			AggregateResponse.Builder pair = AggregateResponse.newBuilder();
			pair.addFirstPart(first);
			response = pair.build();

		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		}catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.info("Row counter from this region is "	+ env.getRegion().getRegionInfo().getRegionNameAsString()
				+ ": " + rowNum);
		done.run(response);
		
	}

	private Integer buildRowCountAgg(AggregateStgRequest request, Scan scan, String range) throws IOException {
		FilterList fl=new FilterList();
		fl.addFilter(new FirstKeyOnlyFilter());
		if(scan.getFilter()!=null)
		fl.addFilter(scan.getFilter());
		scan.setFilter(fl);
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		Integer rowNum=0;
		do {
			hasMoreRows = rs.next(results);
			if (!results.isEmpty()){
				rowNum++;
			}
			results.clear();
		} while (hasMoreRows);
		return rowNum;
	}

	@Override
	public void getAvg(RpcController controller, AggregateStgRequest request,
			RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		RegionScanner rs = null;
		Std std=new Std(BigDecimal.ZERO, 0);
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkCol(scan, request);
			if (request.getScanRangeCount() == 0)
				buildAvgAgg(request, std, scan, null);
			else {
				for (String range : request.getScanRangeList()) {
					buildAvgAgg(request, std, scan, range);
				}
			}
			ByteString first = ByteString.copyFrom(HbaseUtil.kyroSeriLize(std, 4096));
			AggregateResponse.Builder pair = AggregateResponse.newBuilder();
			pair.addFirstPart(first);
			response = pair.build();

		} catch (IOException e) {
			log.error(e.getMessage(),e);
			ResponseConverter.setControllerException(controller, e);
		}catch(LinkageError error){
			log.error(error.getMessage(),error);
			throw error;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (IOException ignored) {
				}
			}
		}
		log.debug("avg from this region is " + env.getRegion().getRegionInfo().getRegionNameAsString() + ": " + std.getSum());
		done.run(response);
	}
	
	
	private void checkCol(Scan scan, AggregateStgRequest request) throws HbaseAggCheckException {
		byte[] colFamily = scan.getFamilies()[0];
		NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
		if(qualifiers==null||!qualifiers.contains(request.getAggTypeQulalifier().getQulifier().getBytes())){
			throw new HbaseAggCheckException(
					"scan does not contain qulifier");
		}
	}
	
	
	@SuppressWarnings("deprecation")
	private BigDecimal buildSumAgg(AggregateStgRequest request,
			Scan scan, String range) throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		BigDecimal lb=BigDecimal.ZERO;
		do {
			hasMoreRows = rs.next(results);
			if (!results.isEmpty()){
				byte[] b = null;
				for (Cell c : results) {
					if (Bytes.toString(c.getQualifier()).equals(
							request.getAggTypeQulalifier().getQulifier())) {
						b = c.getValue();
						break;
					}
				}
				if(b==null)
					throw new HbaseAggUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
				try {
					lb=lb.add(BigDecimal.valueOf(Double.valueOf(Bytes.toString(b))));
				} catch (NumberFormatException e) {
					throw new HbaseAggConvertException(e.getMessage());
				}
			
			}
			results.clear();
		} while (hasMoreRows);
		return lb;
	}
	
	@SuppressWarnings("deprecation")
	private RegionScanner buildAvgAgg(AggregateStgRequest request,
			Std std, Scan scan, String range) throws IOException {
		buildGroupScanRange(scan, range);
		RegionScanner rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
		List<Cell> results = new ArrayList<Cell>();
		boolean hasMoreRows = false;
		do {
			hasMoreRows = rs.next(results);
			if (!results.isEmpty()){
				byte[] b = null;
				for (Cell c : results) {
					if (Bytes.toString(c.getQualifier()).equals(
							request.getAggTypeQulalifier().getQulifier())) {
						b = c.getValue();
						break;
					}
				}
				try {
					std.setSum(std.getSum().add(BigDecimal.valueOf(Double.parseDouble(Bytes.toString(b)))));
					std.setTotal(std.getTotal()+1);
				} catch (NumberFormatException e) {
					throw new HbaseAggConvertException(e.getMessage());
				}catch (NullPointerException e) {
					throw new HbaseAggUnKnowColumnException(request.getAggTypeQulalifier().getQulifier()+" does not exist!");
				}
				
			}
			results.clear();
		} while (hasMoreRows);
		return rs;
	}
	
	
	private void buildGroupScanRange(Scan scan, String range) {
		if (!StringUtils.isBlank(range)) {
			scan.setStartRow(new StringBuilder(range)
					.append(Constants.REGTABLEHBASESTART).toString().getBytes());
			scan.setStopRow(new StringBuilder(range)
					.append(Constants.REGTABLEHBASESTOP).toString().getBytes());
		}
	}
	
}
