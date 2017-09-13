package org.apache.hadoop.hbase.ddl;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.IdxHbaseClient;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

@SuppressWarnings("deprecation")
public class AbstractHbaseDDLClient {
	private static final Logger logger = LoggerFactory.getLogger(AbstractHbaseDDLClient.class);
	protected static HBaseAdmin admin;
	protected static IdxHbaseClient idxHbaseClient;
	protected static Map<String,HTableDescriptor> mapCache;
	public static Map<String, HTableDescriptor> getMapCache() {
		return mapCache;
	}

	public static void setMapCache(Map<String, HTableDescriptor> mapCache) {
		AbstractHbaseDDLClient.mapCache = mapCache;
	}

	public synchronized static void init(Configuration cf) throws IOException{
		if(admin==null){
			try {
				admin=new HBaseAdmin(cf);
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
				throw e;
			}
			idxHbaseClient=new IdxHbaseClient(cf,null);
			initMapCache();
		}
	}
	
	public static void initMapCache() throws IOException {
		HTableDescriptor[] hdts=admin.listTables();
		if(hdts==null||hdts.length==0)
			return;
		mapCache=new HashMap<String,HTableDescriptor>(hdts.length);
		for(HTableDescriptor hdt:hdts){
			if(!hdt.getNameAsString().startsWith(Constants.IDXTABLENAMEESPREFIX))
			mapCache.put(hdt.getNameAsString(), hdt);
		}
	}
	
	public static RowKeyComposition getRKCFromMapCache(String hbaseTable,String hcd) {
		HTableDescriptor hdt= mapCache.get(hbaseTable);
		RowKeyComposition rkc=JSON.parseObject(hdt.getValue(hcd), RowKeyComposition.class);
		return rkc;
	}
	
	protected byte[][] buildKey(LinkedHashSet<String> setKey) {
		if(CollectionUtils.isEmpty(setKey))
		return null;
		byte[][] bb=new byte[setKey.size()][];
		int i=0;
		for(String k:setKey){
			bb[i++]=Bytes.toBytes(k);
		}
		return bb;
	}
	
	protected byte[][] genSplitKey(int regionNum) {
		byte[][] bb=new byte[regionNum-1][];
		for(int i=0;i<regionNum-1;i++){
			bb[i]=Bytes.toBytes(Long.toString((i+1),36));
		}
		return bb;
	}
}