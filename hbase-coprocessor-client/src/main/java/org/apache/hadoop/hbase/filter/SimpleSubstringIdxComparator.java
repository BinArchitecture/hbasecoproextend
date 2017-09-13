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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ScanOrderStgKV;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
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
public class SimpleSubstringIdxComparator extends ByteArrayComparable {

	private ScanOrderStgKV sosv;
	private static final Log LOG = LogFactory
			.getLog(SimpleSubstringIdxComparator.class.getName());

	/**
	 * Constructor
	 * 
	 * @param substr
	 *            the substring
	 * @throws IOException 
	 */
	public SimpleSubstringIdxComparator(ScanOrderStgKV sosv) throws IOException {
		super(HbaseUtil.kyroSeriLize(sosv, 4096));
		this.sosv = sosv;
	}

	@Override
	public byte[] getValue() {
		try {
			return HbaseUtil.kyroSeriLize(sosv, 4096);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public int compareTo(byte[] value, int offset, int length) {
		try {
			if (sosv == null)
				return 1;
			if (sosv.getMaxsok() != null || sosv.getMinsok() != null) {
				String row = Bytes.toString(value, offset, length);
				String[] ss = row.split(Constants.SPLITTER);
				String[] k = ss[0].split(Constants.QLIFIERSPLITTER);
				if (sosv.getMaxsok() != null) {
					if (k[0].equals(sosv.getMaxsok().getQulifier())) {
						if (k[1].compareTo(sosv.getMaxsok().getValue()) <= 0) {
							if (sosv.getMinsok() == null)
								return 0;
							else {
								return k[1].compareTo(sosv.getMinsok()
										.getValue()) >= 0 ? 0 : 1;
							}
						}
					} else {
						return 1;
					}
				}
				if (sosv.getMinsok() != null) {
					if (k[0].equals(sosv.getMinsok().getQulifier())) {
						return k[1].compareTo(sosv.getMinsok().getValue()) >= 0 ? 0
								: 1;
					} else {
						return 1;
					}
				}
			}
			return 1;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return 1;
		}
	}

	public ScanOrderStgKV getSosv() {
		return sosv;
	}

	public void setSosv(ScanOrderStgKV sosv) {
		this.sosv = sosv;
	}

	/**
	 * @return The comparator serialized using pb
	 */
	public byte[] toByteArray() {
		CoProcessorComparatorProtos.ComplexSubstringComparator.Builder builder = CoProcessorComparatorProtos.ComplexSubstringComparator
				.newBuilder();
		try {
			builder.setListScan(ByteString.copyFrom(HbaseUtil.kyroSeriLize(sosv, 4096)));
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return builder.build().toByteArray();
	}

	/**
	 * @param pbBytes
	 *            A pb serialized {@link SimpleSubstringIdxComparator} instance
	 * @return An instance of {@link SimpleSubstringIdxComparator} made from
	 *         <code>bytes</code>
	 * @throws DeserializationException
	 * @see #toByteArray
	 */
	public static SimpleSubstringIdxComparator parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		CoProcessorComparatorProtos.SimpleSubstringIdxComparator proto;
		try {
			proto = CoProcessorComparatorProtos.SimpleSubstringIdxComparator
					.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		SimpleSubstringIdxComparator sc=null;
		try {
			sc = new SimpleSubstringIdxComparator(
					HbaseUtil.kyroDeSeriLize(proto.getSosv().toByteArray(), ScanOrderStgKV.class));
		} catch (Exception e) {
			throw new DeserializationException(e);
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
		if (!(other instanceof SimpleSubstringIdxComparator))
			return false;
		SimpleSubstringIdxComparator comparator = (SimpleSubstringIdxComparator) other;
		return super.areSerializedFieldsEqual(comparator)
				&& this.sosv.equals(comparator.sosv);
	}
}