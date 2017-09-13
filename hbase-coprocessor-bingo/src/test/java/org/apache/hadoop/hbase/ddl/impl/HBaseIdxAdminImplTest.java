package org.apache.hadoop.hbase.ddl.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.coprocessor.IdxHbaseClient;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.model.FamilyCond;
import org.apache.hadoop.hbase.client.coprocessor.model.MainHbaseCfPk;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseCrTableParam;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.OrderBy;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseLinkedHashSet;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseTreeSet;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.apache.hadoop.hbase.ddl.HBaseDDLAdminInterface;
import org.apache.hadoop.hbase.ddl.HBaseIdxAdminInterface;
import org.junit.Before;
import org.junit.Test;


public class HBaseIdxAdminImplTest {

	HBaseIdxAdminInterface hia;
	HBaseDDLAdminInterface hda;
	AggregationClient client;
	IdxHbaseClient idxClient;
	@Before
	public void init() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "centos7MRBP");
		conf.setLong("hbase.rpc.timeout", 30000);
		conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
				60000 );
		conf.set("hbase.client.pause", "50");
		conf.set("hbase.client.retries.number", "5");
		conf.setLong("hbase.client.scanner.caching", 20
				);
		try {
			hia=new HBaseIdxAdminImpl();
			hda=new HBaseDDLAdminImpl();
			client=new AggregationClient(conf);
			idxClient=new IdxHbaseClient(conf, null);
			AbstractHbaseDDLClient.init(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void crIdx() {
		try {
//			ColumnInterpreter<Long, Long,EmptyMsg, LongMsg, LongMsg> ci=new LongColumnInterpreter();
//			client.rowCount(TableName.valueOf("omsorder"), ci, new Scan());
			HBaseIdxParam param=new HBaseIdxParam().build("omsorder", "idxOrderCrTime", "orderdetail", Arrays.asList(new String[]{"createTime"}),null);
			hia.addIdx(param);
			System.out.println();
//			Scan scan=new Scan();
//			scan.addFamily("orderdetail".getBytes());
//			client.rowCount(TableName.valueOf("omsorder"), null, scan, null);
//			idxClient.addIdx(TableName.valueOf("omsorder"), "idxOrderCrTime", "orderdetail", "createTime",new Scan());
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void crTb() {
		try {
			TreeSet<String> orderdetailSet=new TreeSet<String>();
			TreeSet<String> olSet=new TreeSet<String>();
			TreeSet<String> oldetailSet=new TreeSet<String>();
			TreeSet<String> oshipSet=new TreeSet<String>();
			orderdetailSet.add("boid");
			orderdetailSet.add("auid");
//			orderdetailSet.add("createTime");
			HbaseDataType orderBy=new HbaseDataType();
			orderBy.setDataType(Date.class);
			orderBy.setOrderBy(OrderBy.DESC);
			orderBy.setQulifier("createTime");
			
			olSet.add("olid");
			oldetailSet.add("oldid");
			oshipSet.add("osid");
			RowKeyComposition rkc=new RowKeyComposition(new MainHbaseCfPk("boid", 12),orderdetailSet,null);
			LinkedHashSet<String> familyColsNeedIdx=new LinkedHashSet<String>(3);
			familyColsNeedIdx.add("auid");
//			familyColsNeedIdx.add("createTime");
			familyColsNeedIdx.add("boid");
			rkc.setFamilyColsNeedIdx(familyColsNeedIdx);
			HBaseCrTableParam param=new HBaseCrTableParam().build("omsorder", new FamilyCond[]{new FamilyCond("orderdetail",rkc),
					new FamilyCond("orderline",new RowKeyComposition(new MainHbaseCfPk("olid", 12),olSet,null)),
					new FamilyCond("orderlinedetail",new RowKeyComposition(new MainHbaseCfPk("oldid", 12),oldetailSet,null)),
					new FamilyCond("ordershipment",new RowKeyComposition(new MainHbaseCfPk("osid", 12),oshipSet,null))}, 36,"orderdetail", false);
			hda.creatTable(param);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void crOrderTb() {
HbaseTreeSet<String> omsorder=new HbaseTreeSet<String>();
		
		omsorder.add("createdate");
		omsorder.add("outorderid");
		omsorder.add("basestore");
		omsorder.add("parentorder");
		omsorder.add("issuedate");
		omsorder.add("paymentDate");
		omsorder.add("scheduledshippingdate");
		omsorder.add("username");
		omsorder.add("state");
		omsorder.add("mergenumber");
		omsorder.add("orderid");
		omsorder.add("shad_mobilephone");
		omsorder.add("shad-name");
		omsorder.add("shad-phonenumber");
		
		
		RowKeyComposition rkc=new RowKeyComposition(new MainHbaseCfPk("orderid", 21),omsorder,null);
		HbaseLinkedHashSet<String> familyColsNeedIdx=new HbaseLinkedHashSet<String>(14);
		familyColsNeedIdx.add("orderid");
		familyColsNeedIdx.add("mergenumber");
		familyColsNeedIdx.add("parentorder");
		familyColsNeedIdx.add("outorderid");
		familyColsNeedIdx.add("shad-mobilephone");
		familyColsNeedIdx.add("shad-phonenumber");
		familyColsNeedIdx.add("username");
		familyColsNeedIdx.add("shad-name");
		familyColsNeedIdx.add("issuedate");
		familyColsNeedIdx.add("paymentdate");
		familyColsNeedIdx.add("createdate");
		familyColsNeedIdx.add("scheduledshippingdate");
		familyColsNeedIdx.add("basestore");
		familyColsNeedIdx.add("state");
		rkc.setFamilyColsNeedIdx(familyColsNeedIdx);
		
		//#####################################################
		HbaseTreeSet<String> orderlineSet=new HbaseTreeSet<String>();
		orderlineSet.add("olid");
//		orderlineSet.add("myorder");
		orderlineSet.add("skuid");
		
		RowKeyComposition rkcLine=new RowKeyComposition(new MainHbaseCfPk("olid", 21),orderlineSet,null);
		HbaseLinkedHashSet<String> familyColsNeedIdxLine=new HbaseLinkedHashSet<String>(1);
//		familyColsNeedIdxLine.add("myorder");
		familyColsNeedIdxLine.add("skuid");
		rkcLine.setFamilyColsNeedIdx(familyColsNeedIdxLine);
		HBaseCrTableParam param=new HBaseCrTableParam().build("hbaseorder", new FamilyCond[]{new FamilyCond("order",rkc),
				new FamilyCond("orderline",rkcLine)}, 36,"order", false);
		try {
			hda.creatTable(param);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
