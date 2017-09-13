package org.apache.hadoop.hbase.coprocessor.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ShellUtil.class);
	
	private static final int timeout = 10000; 
	
	/**
	 * 运行一个外部命令，返回状态.若超过指定的超时时间，抛出TimeoutException
	 * 
	 */
	public static ProcessStatus execute(final String command)
			throws IOException, InterruptedException, TimeoutException {
		
		ProcessBuilder pb = new ProcessBuilder("/bin/sh","-c",command);
		pb.redirectErrorStream(true);
		Process process = pb.start();
		
		Worker worker = new Worker(process);
		worker.start();
		ProcessStatus ps = worker.getProcessStatus();
		try {
			worker.join(timeout);
			if (ps.exitCode == ProcessStatus.CODE_STARTED) {
				// not finished
				worker.interrupt();
				throw new TimeoutException();
			} else {
				return ps;
			}
		} catch (InterruptedException e) {
			// canceled by other thread.
			worker.interrupt();
			throw e;
		} finally {
			process.destroy();
		}
	}
	
	private static class Worker extends Thread {
		private final Process process;
		private ProcessStatus ps;
		
		private Worker(Process process) {
			this.process = process;
			this.ps = new ProcessStatus();
		}
		
		public void run() {
			try {
				InputStream is = process.getInputStream();
				try {
					ps.output = IOUtils.toString(is);
				} catch (IOException ignore) { }
				ps.exitCode = process.waitFor();
				
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		
		public ProcessStatus getProcessStatus() {
			return this.ps;
		}
	}
	
	public static class ProcessStatus {
		public static final int CODE_STARTED = -257;
		public volatile int exitCode;
		public volatile String output;
	}

	@Deprecated
	public static String RunShell(String shpath) throws IOException, InterruptedException {   
		Process ps = Runtime.getRuntime().exec(shpath);  
		ps.waitFor();  
		BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));  
		StringBuffer sb = new StringBuffer();  
		String line;  
		while ((line = br.readLine()) != null) {  
			sb.append(line).append("\n");  
		}
		if (sb.length() > 0) {
			sb.substring(0, sb.length() - 2);
		}
		String result = sb.toString();  
		LOG.info(result); 
		return result;
    }
}
