package org.apache.hadoop.hbase.ddl.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDDLException;
import org.apache.hadoop.hbase.client.coprocessor.model.HbaseOp;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseCrTableParam;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseDDLResult;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseModifiedParam;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.apache.hadoop.hbase.ddl.HBaseDDLAdminInterface;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.fastjson.JSON;

@SuppressWarnings("deprecation")
@Service(protocol = { "rest" }, timeout = 200000)
public class HBaseDDLAdminImpl extends AbstractHbaseDDLClient implements
		HBaseDDLAdminInterface {
	private static final Logger logger = LoggerFactory
			.getLogger(HBaseDDLAdminImpl.class);

	public HBaseDDLResult creatTable(HBaseCrTableParam param) throws Exception {
		HBaseDDLResult result = new HBaseDDLResult();
		if (admin.tableExists(param.getTableName())) {
			logger.info("table " + param.getTableName() + " Exists!");
			result.setResult("table " + param.getTableName() + " Exists!");
			return result;
		} else {
			HTableDescriptor desc = new HTableDescriptor(param.getTableName());
			for (int i = 0; i < param.getFamilyCrArray().length; i++) {
				HColumnDescriptor hcd = new HColumnDescriptor(
						param.getFamilyCrArray()[i].getFamilyName());
				hcd.setDataBlockEncoding(DataBlockEncoding.PREFIX);
				hcd.setInMemory(true);
				hcd.setMaxVersions(1);
				hcd.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
				if (param.isCompress()) {
					hcd.setCompressionType(Algorithm.SNAPPY);
					hcd.setCompactionCompressionType(Algorithm.SNAPPY);
				}
				desc.addFamily(hcd);
				desc.setValue(param.getFamilyCrArray()[i].getFamilyName(),
						JSON.toJSONString(param.getFamilyCrArray()[i].getRkc()));
			}
			desc.setValue(Constants.HBASEES2NDTOTAL, String.valueOf(param.getEs2ndTotal()));
			if (param.getRegionNum() > 1 && param.getRegionNum() < 37) {
				desc.setRegionSplitPolicyClassName(Constants.HBASETBSPLITCLASSNAME);
				desc.setValue(Constants.DELIMITER_KEY, Constants.SPLITTER);
				desc.setValue(Constants.HBASEMAINTABLE,
						param.getMainFamilyName());
				byte[][] splitkey = genSplitKey(param.getRegionNum());
				try {
					admin.createTable(desc, splitkey);
					result.setResult("table " + param.getTableName()
							+ " has created success!");
					return result;
				} catch (Exception e) {
					if (e instanceof TableExistsException) {
						logger.info("create table Success!");
						result.setResult("table " + param.getTableName()
								+ " has created success!");
						return result;
					}
					logger.error(e.getMessage(), e);
					result.setExcp(new HbaseDDLException(e.getMessage()));
					return result;
				}
			}
			admin.createTable(desc);
			result.setResult("table " + param.getTableName()
					+ " has been created success!");
			return result;
		}
	}

	public HBaseDDLResult deleteTable(String tableName) throws IOException {
		HBaseDDLResult result = new HBaseDDLResult();
		try {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			result.setResult("table " + tableName
					+ " has been deleted success!");
			return result;
		} catch (Exception e) {
			if (e instanceof TableNotFoundException) {
				logger.info("delete table Success!");
				result.setResult("table " + tableName
						+ " has been deleted success!");
				return result;
			}
			logger.error(e.getMessage(), e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		}
	}

	public HBaseDDLResult modifyTableFamily(HBaseModifiedParam param)
			throws IOException {
		HTable table = null;
		HBaseDDLResult result = new HBaseDDLResult();
		try {
			table = new HTable(admin.getConfiguration(), param.getTableName());
			admin.disableTable(param.getTableName());
			if (HbaseOp.ADD.equals(param.getOp())) {
				HColumnDescriptor hdc = new HColumnDescriptor(
						Bytes.toBytes(param.getFc().getFamilyName()));
				if (param.getFc().getRkc() != null)
					hdc.setValue(param.getFc().getFamilyName(),
							JSON.toJSONString(param.getFc().getRkc()));
				admin.addColumn(param.getTableName(), hdc);
			} else if (HbaseOp.REMOVE.equals(param.getOp())) {
				admin.deleteColumn(param.getTableName(), param.getFc()
						.getFamilyName());
			}
			if (admin.isTableDisabled(param.getTableName()))
				admin.enableTable(param.getTableName());
			result.setResult("operation success!");
			return result;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		} finally {
			table.close();
		}
	}

	public HBaseDDLResult modifyTableCoprocessor(HBaseModifiedParam param)
			throws IOException {
		HTable table = null;
		HBaseDDLResult result = new HBaseDDLResult();
		try {
			table = new HTable(admin.getConfiguration(), param.getTableName());
			HTableDescriptor descriptor = new HTableDescriptor(
					table.getTableDescriptor());
			buildCoprocessor(param.getOp(), descriptor,
					param.getCoproceeClassName());
			admin.disableTable(param.getTableName());
			admin.modifyTable(Bytes.toBytes(param.getTableName()), descriptor);
			admin.enableTable(param.getTableName());
			result.setResult("operation success!");
			return result;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		} finally {
			table.close();
		}
	}

	private void buildCoprocessor(HbaseOp op, HTableDescriptor descriptor,
			String coproceeClassName) {
		if (HbaseOp.ADD.equals(op)) {
			try {
				descriptor.addCoprocessor(coproceeClassName);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		} else if (HbaseOp.REMOVE.equals(op)) {
			descriptor.removeCoprocessor(coproceeClassName);
		}
	}

	@Override
	public HBaseDDLResult getTableDesc(HBaseIdxParam param) throws Exception {
		HBaseDDLResult result = new HBaseDDLResult();
		AbstractHbaseDDLClient.initMapCache();
		HTableDescriptor hdt = mapCache.get(param.getTableName());
		if (hdt == null)
			result.setExcp(new HbaseDDLException("no such table:"
					+ param.getTableName()));
		String rrs = hdt.getValue(param.getFamilyName());
		if (rrs == null)
			result.setExcp(new HbaseDDLException("no such table or family:"
					+ param.getTableName() + "." + param.getFamilyName()));
		RowKeyComposition rkc = JSON.parseObject(rrs, RowKeyComposition.class);
		result.setRkc(rkc);
		return result;
	}

	@Override
	public HBaseDDLResult listTableDesc() throws Exception {
		HBaseDDLResult result=new HBaseDDLResult();
		result.buildMap(admin);
		return result;
	}
}