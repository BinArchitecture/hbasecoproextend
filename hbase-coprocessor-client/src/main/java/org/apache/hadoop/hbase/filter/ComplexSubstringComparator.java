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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ListScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.RegexStringComparator.Engine;
import org.apache.hadoop.hbase.filter.RegexStringComparator.JavaRegexEngine;
import org.apache.hadoop.hbase.protobuf.generated.CoProcessorComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This comparator is for use with SingleColumnValueFilter, for filtering based
 * on the value of a given column. Use it to test if a given substring appears
 * in a cell value in the column. The comparison is case insensitive.
 * <p>
 * Only EQUAL or NOT_EQUAL tests are valid with this comparator.
 * <p>
 * For example:
 * <p>
 * 
 * <pre>
 * SingleColumnValueFilter scvf = new SingleColumnValueFilter(&quot;col&quot;,
 * 		CompareOp.EQUAL, new SubstringComparator(&quot;substr&quot;));
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ComplexSubstringComparator extends ByteArrayComparable {

	private ListScanOrderedKV listScan;
	private Engine engine;
	private static final Log LOG = LogFactory
			.getLog(ComplexSubstringComparator.class.getName());

	/**
	 * Constructor
	 * 
	 * @param substr
	 *            the substring
	 * @throws IOException
	 */
	public ComplexSubstringComparator(ListScanOrderedKV listScan)
			throws IOException {
		super(HbaseUtil.kyroSeriLize(listScan, -1));
		this.listScan = listScan;
		if (!StringUtils.isBlank(listScan.getExpr()))
			this.engine = new JavaRegexEngine(listScan.getExpr(),
					Pattern.DOTALL);
	}

	@Override
	public byte[] getValue() {
		try {
			return HbaseUtil.kyroSeriLize(listScan, -1);
		} catch (IOException e) {
		}
		return null;
	}

	@Override
	public int compareTo(byte[] value, int offset, int length) {
		try {
			if (listScan == null)
				return 1;
			int boo = 1;
			String row = Bytes.toString(value, offset, length);
			if (listScan.getListScan() != null) {
				Map<String, String> conmap = HbaseUtil.buildConMapByRowKey(row);
				for (ScanOrderedKV sok : listScan.getListScan()) {
					String s = conmap.get(sok.getQulifier());
					if (s == null)
						return 1;
					int compareResult = s.compareTo(sok.getValue());
					boolean mark = false;
					switch (sok.getOp()) {
					case LESS:
						mark = compareResult < 0;
						break;
					case LESS_OR_EQUAL:
						mark = compareResult <= 0;
						break;
					case EQUAL:
						mark = compareResult == 0;
						break;
					case NOT_EQUAL:
						mark = compareResult != 0;
						break;
					case GREATER_OR_EQUAL:
						mark = compareResult >= 0;
						break;
					case GREATER:
						mark = compareResult > 0;
						break;
					default:
					}
					if (!mark)
						return 1;
				}
				boo = StringUtils.isBlank(listScan.getSubStr()) ? 0 : row
						.contains(listScan.getSubStr()) ? 0 : 1;
			} else {
				if (!StringUtils.isBlank(listScan.getExpr()) && engine != null)
					boo = StringUtils.isBlank(listScan.getSubStr()) ? 0 : row
							.contains(listScan.getSubStr()) ? 0 : 1;
				else
					boo = StringUtils.isBlank(listScan.getSubStr()) ? 1 : row
							.contains(listScan.getSubStr()) ? 0 : 1;
			}
			if (!StringUtils.isBlank(listScan.getExpr()) && engine != null) {
				boo |= engine.compareTo(value, offset, length);
			}
			return boo;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return 1;
		}
	}

	public ListScanOrderedKV getListScan() {
		return listScan;
	}

	public void setListScan(ListScanOrderedKV listScan) {
		this.listScan = listScan;
	}

	/**
	 * @return The comparator serialized using pb
	 */
	public byte[] toByteArray() {
		CoProcessorComparatorProtos.ComplexSubstringComparator.Builder builder = CoProcessorComparatorProtos.ComplexSubstringComparator
				.newBuilder();
		try {
			builder.setListScan(ByteString.copyFrom(HbaseUtil.kyroSeriLize(
					listScan, 81920)));
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return builder.build().toByteArray();
	}

	/**
	 * @param pbBytes
	 *            A pb serialized {@link ComplexSubstringComparator} instance
	 * @return An instance of {@link ComplexSubstringComparator} made from
	 *         <code>bytes</code>
	 * @throws DeserializationException
	 * @see #toByteArray
	 */
	public static ComplexSubstringComparator parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		CoProcessorComparatorProtos.ComplexSubstringComparator proto;
		try {
			proto = CoProcessorComparatorProtos.ComplexSubstringComparator
					.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		ComplexSubstringComparator sc = null;
		try {
			sc = new ComplexSubstringComparator(HbaseUtil.kyroDeSeriLize(proto
					.getListScan().toByteArray(), ListScanOrderedKV.class));
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return sc;
	}

	/**
	 * @param other
	 * @return true if and only if the fields of the comparator that are
	 *         serialized are equal to the corresponding fields in other. Used
	 *         for testing.
	 */
	boolean areSerializedFieldsEqual(ByteArrayComparable other) {
		if (other == this)
			return true;
		if (!(other instanceof ComplexSubstringComparator))
			return false;

		ComplexSubstringComparator comparator = (ComplexSubstringComparator) other;
		return super.areSerializedFieldsEqual(comparator)
				&& this.listScan.equals(comparator.listScan);
	}

//	public static void main(String[] args) {
//		String row = "z00000001_t_basestore::single!!BaseStoreData!!1033_createdate::2015-07-31 16:05:01.0_issuedate::2015-07-31 15:59:39.0_mergenumber::NULL_orderid::0000FX150731000000001_outorderid::155015073046960_parentorder::NULL_paymentdate::2015-07-31 15:59:39.0_scheduledshippingdate::2015-08-01 16:05:01.0_shad-mobilephone::18711095899_shad-name::430104_shad-phonenumber::_state::OUT$-$STORAGE_username::";
//		Map<String, String> conmap =  HbaseUtil.buildConMapByRowKey(row);
//		System.out.println();
//	}
}