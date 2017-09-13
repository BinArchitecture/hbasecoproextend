package org.apache.hadoop.hbase.ddl;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hbase.client.HbaseClientInterface;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseCrTableParam;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseDDLResult;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseModifiedParam;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;
@Path("/hbaseddl")
@Consumes({MediaType.APPLICATION_JSON})
@Produces({ ContentType.APPLICATION_JSON_UTF_8})
public interface HBaseDDLAdminInterface extends HbaseClientInterface{
	@Path("crTb")
	@POST
	public HBaseDDLResult creatTable(HBaseCrTableParam param)
			throws Exception;
	
	@Path("delTb")
	@POST
	public HBaseDDLResult deleteTable(String tableName) throws IOException;
	
	@Path("updateTbFamily")
	@POST
	public HBaseDDLResult modifyTableFamily(HBaseModifiedParam param)
			throws IOException;
	
	@Path("updateTbCoprocessor")
	@POST
	public HBaseDDLResult modifyTableCoprocessor(HBaseModifiedParam param) throws IOException;
	
	@Path("getTableDesc")
	@POST
	public HBaseDDLResult getTableDesc(HBaseIdxParam param) throws Exception;
	
	@Path("listTableDesc")
	@POST
	public HBaseDDLResult listTableDesc() throws Exception;
}
