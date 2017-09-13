package org.apache.hadoop.hbase.coprocessor.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseDDLResult;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.lppz.util.StringUtils;
import com.lppz.util.http.BaseHttpClientsComponent;
import com.lppz.util.http.FutureWrapper;
import com.lppz.util.http.enums.HttpMethodEnum;
import com.lppz.util.kryo.KryoUtil;

public class BaseHbaseHttpClient extends BaseHttpClientsComponent {
	protected RecoverableZooKeeper rz;
	private static final Logger logger = LoggerFactory.getLogger(BaseHbaseHttpClient.class);
	protected static Map<String,Map<String,String>> mapCache;
	
	public synchronized void init(RecoverableZooKeeper rz) {
		try {
//			super.init();
			this.rz=rz;
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}
	
	protected HBaseDDLResult doDDL(String hostSuffix,String entity) throws Exception {
		super.initHttpClient();
		String hostPort = getClusterdDDLHostByZk();
		HttpRequestBase httpPost = super.createReqBase("http://" + hostPort
				+ hostSuffix, HttpMethodEnum.POST);
		if(entity!=null){
			StringEntity s = new StringEntity(entity, "UTF-8");
			s.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE,
					"application/json"));
			((HttpPost) httpPost).setEntity(s);
		}
		FutureWrapper fwPost = super.doHttpExec(httpPost, null, 0);
		try {
			HttpResponse hrPost = fwPost.getFh().get();
			HBaseDDLResult result = JSON.parseObject(StringUtils
					.convertStreamToString(hrPost.getEntity().getContent()),
					HBaseDDLResult.class);
			if (result.getExcp() != null)
				throw result.getExcp();
			return result;
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw e;
		} finally {
			super.closeHttpClient();
		}
	}
	
	private String getClusterdDDLHostByZk(){
		if(rz==null)
			return null;
		try {
			List<String> listHost=new ArrayList<String>();
			List<String> paths=rz.getChildren(Constants.ZOOBINGOADMIN, false);
			if(CollectionUtils.isNotEmpty(paths)){
				for(String path:paths){
					byte[] b=rz.getData(Constants.ZOOBINGOADMIN+"/"+path, true, null);
					listHost.add(KryoUtil.kyroDeSeriLize(b, String.class));
				}
				int i=ThreadLocalRandom.current().nextInt(listHost.size());
				return listHost.get(i);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return null;
	}
	
	public RowKeyComposition getRKCFromMapCache(String hbaseTable,String hcd) {
		HBaseIdxParam param=new HBaseIdxParam().build(hbaseTable, null, hcd, null);
		HBaseDDLResult result;
		try {
			result = doDDL("/services/hbaseddl/getTableDesc", JSON.toJSONString(param));
			if(result==null)
				return null;
			return result.getRkc();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return null;
	}
}