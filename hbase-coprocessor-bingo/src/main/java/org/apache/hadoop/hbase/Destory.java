package org.apache.hadoop.hbase;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.dubbo.config.annotation.Service;
import com.lppz.dubbo.BaseMicroServiceComponent;
import com.lppz.dubbo.MicroServiceDestoryInterface;
import com.lppz.util.kafka.consumer.KafkaBaseRunner;

@Service(protocol = "rest", timeout = 100000)
public class Destory extends BaseMicroServiceComponent implements MicroServiceDestoryInterface {
	static Logger logger = LoggerFactory.getLogger(Destory.class);

	public static void main(String[] args) {
		Destory destory = new Destory();
		destory.shutdown();
	}

	@Override
	public boolean close(boolean boo) {
		if(!boo)
			return false;
		HBingoServer.flag=false;
		KafkaBaseRunner.needRun=true;
		return true;
	}

	@Override
	protected String generateCloseRestUrl() {
		StringBuilder sb=new StringBuilder();
		
		sb.append(LOCALHOST).append(PORT).append(SEPARATOR).append(WEB_CONTEXT_PATH).append(DESTORY_PATH);
		
		return sb.toString();
	}
}