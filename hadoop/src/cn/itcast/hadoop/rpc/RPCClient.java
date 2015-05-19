package cn.itcast.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	public static void main(String[] args) throws IOException {
		Barty proxy = RPC.getProxy(Barty.class, 10010,
				new InetSocketAddress("192.168.8.100", 9527), new Configuration());
		String sayHi = proxy.sayHi("tomcat");
		System.out.println(sayHi);
		
	}

}
