/**
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

package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.PartRowKeyString;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ComplexFilterProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This filter is used to filter based on the key. It takes an operator (equal,
 * greater, not equal, etc) and a byte [] comparator for the row, and column
 * qualifier portions of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known row range needs to be scanned, use
 * {@link org.apache.hadoop.hbase.CellScanner} start and stop rows directly
 * rather than a filter.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ComplexRowFilter extends CompareFilter {
	private static final Log log = LogFactory
			.getLog(ComplexRowFilter.class);
	private boolean filterOutRow = false;
	private PartRowKeyString scanRequireCond;
	private String idxDescfamily;
	private CasCadeScanMap postScanNext;
	private boolean isFirstPage = true;

	public boolean isFirstPage() {
		return isFirstPage;
	}

	public void setFirstPage(boolean isFirstPage) {
		this.isFirstPage = isFirstPage;
	}

	public String getIdxDescfamily() {
		return idxDescfamily;
	}

	public void setIdxDescfamily(String idxDescfamily) {
		this.idxDescfamily = idxDescfamily;
	}

	public boolean isFilterOutRow() {
		return filterOutRow;
	}

	public void setFilterOutRow(boolean filterOutRow) {
		this.filterOutRow = filterOutRow;
	}

	public PartRowKeyString getScanRequireCond() {
		return scanRequireCond;
	}

	public void setScanRequireCond(PartRowKeyString scanRequireCond) {
		this.scanRequireCond = scanRequireCond;
	}

	public CasCadeScanMap getPostScanNext() {
		return postScanNext;
	}

	public void setPostScanNext(CasCadeScanMap postScanNext) {
		this.postScanNext = postScanNext;
	}

	/**
	 * Constructor.
	 * 
	 * @param rowCompareOp
	 *            the compare op for row matching
	 * @param rowComparator
	 *            the comparator for row matching
	 */
	public ComplexRowFilter(final CompareOp rowCompareOp,
			final ByteArrayComparable rowComparator) {
		super(rowCompareOp, rowComparator);
	}

	@Override
	public void reset() {
		this.filterOutRow = false;
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) {
		if (this.filterOutRow) {
			return ReturnCode.NEXT_ROW;
		}
		return ReturnCode.INCLUDE;
	}

	@Override
	public boolean filterRowKey(byte[] data, int offset, int length) {
		if (doCompare(this.compareOp, this.comparator, data, offset, length)) {
			this.filterOutRow = true;
		}
		return this.filterOutRow;
	}

	@Override
	public boolean filterRow() {
		return this.filterOutRow;
	}

	public static Filter createFilterFromArguments(
			ArrayList<byte[]> filterArguments) {
		@SuppressWarnings("rawtypes")
		// for arguments
		ArrayList arguments = CompareFilter.extractArguments(filterArguments);
		CompareOp compareOp = (CompareOp) arguments.get(0);
		ByteArrayComparable comparator = (ByteArrayComparable) arguments.get(1);
		return new ComplexRowFilter(compareOp, comparator);
	}

	/**
	 * @return The filter serialized using pb
	 */
	public byte[] toByteArray() {
		ComplexFilterProtos.ComplexRowFilter.Builder builder = ComplexFilterProtos.ComplexRowFilter
				.newBuilder();
		builder.setCompareFilter(super.convert());
		try {
			if (this.scanRequireCond != null)
				builder.setScanRequireCond(ByteString.copyFrom(HbaseUtil.kyroSeriLize(this.scanRequireCond, -1)));
			if (this.postScanNext != null)
				builder.setPostScanNext(ByteString.copyFrom(HbaseUtil.kyroSeriLize(this.postScanNext, -1)));
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
		if (this.idxDescfamily != null)
			builder.setIdxFamilyName(this.idxDescfamily);
			builder.setIsFirstPage(this.isFirstPage);
		return builder.build().toByteArray();
	}

	/**
	 * @param pbBytes
	 *            A pb serialized {@link ComplexRowFilter} instance
	 * @return An instance of {@link ComplexRowFilter} made from <code>bytes</code>
	 * @throws DeserializationException
	 * @see #toByteArray
	 */
	public static ComplexRowFilter parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		ComplexFilterProtos.ComplexRowFilter proto;
		try {
			proto = ComplexFilterProtos.ComplexRowFilter.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		final CompareOp valueCompareOp = CompareOp.valueOf(proto
				.getCompareFilter().getCompareOp().name());
		ByteArrayComparable valueComparator = null;
		try {
			if (proto.getCompareFilter().hasComparator()) {
				valueComparator = ProtobufUtil.toComparator(proto
						.getCompareFilter().getComparator());
			}
		} catch (IOException ioe) {
			throw new DeserializationException(ioe);
		}
		ComplexRowFilter rf = new ComplexRowFilter(valueCompareOp, valueComparator);
		try {
			if(proto.getScanRequireCond()!=null&&!proto.getScanRequireCond().isEmpty())
			rf.setScanRequireCond(HbaseUtil.kyroDeSeriLize(proto.getScanRequireCond().toByteArray(), PartRowKeyString.class));
			if(proto.getPostScanNext()!=null&&!proto.getPostScanNext().isEmpty())
			rf.setPostScanNext(HbaseUtil.kyroDeSeriLize(proto.getPostScanNext().toByteArray(), CasCadeScanMap.class));
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		rf.setIdxDescfamily(proto.getIdxFamilyName());
		rf.setFirstPage(proto.getIsFirstPage());
		return rf;
	}

	/**
	 * @param other
	 * @return true if and only if the fields of the filter that are serialized
	 *         are equal to the corresponding fields in other. Used for testing.
	 */
	boolean areSerializedFieldsEqual(Filter o) {
		if (o == this)
			return true;
		if (!(o instanceof ComplexRowFilter))
			return false;

		return super.areSerializedFieldsEqual(o);
	}
}
