package org.apache.hadoop.hbase.ddl;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hbase.client.HbaseClientInterface;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseDDLResult;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;

@Path("/hbaseIdxddl")
@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ ContentType.APPLICATION_JSON_UTF_8 })
public interface HBaseIdxAdminInterface extends HbaseClientInterface {
	@Path("addIdx")
	@POST
	public HBaseDDLResult addIdx(HBaseIdxParam param) throws Exception;

	@Path("dropIdx")
	@POST
	public HBaseDDLResult dropIdx(HBaseIdxParam param) throws IOException;

	@Path("addIdxTable")
	@POST
	public HBaseDDLResult addIdxTable(HBaseIdxParam param) throws Exception;

	@Path("dropIdxTable")
	@POST
	public HBaseDDLResult dropIdxTable(HBaseIdxParam param) throws Exception;
	
	@Path("getIdx")
	@POST
	public HBaseDDLResult getIdx(HBaseIdxParam param) throws Exception;
	
	@Path("existIdxData")
	@POST
	public HBaseDDLResult existIdxData(List<EsIdxHbaseType> listId) throws Exception;
}
