package com.shrikant.server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.handler.FileHandler;
import com.shrikant.service.DHTNode;
import com.shrikant.service.DHTNode.Iface;
import com.shrikant.utils.Stabilize;

public class TestServer implements Runnable {
	private int portNumber;
	private int interval;
	private FileHandler fileHandler;
	private DHTNode.Processor<Iface> processor;
	private Thread nodeThread = null;

	public TestServer(int portNumber,int interval){
		this.portNumber = portNumber;
		this.interval = interval;
		fileHandler = new FileHandler(portNumber);
		processor = new DHTNode.Processor<DHTNode.Iface>(fileHandler);
	}
	
	public void startServer(){
		
		if(nodeThread==null){
			nodeThread = new Thread(this,Integer.toString(portNumber));
			nodeThread.start();
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			
			Stabilize stabilize= new Stabilize(portNumber,interval);
			stabilize.start();
			System.out.println("Staring the file server at port:"+portNumber);
			server.serve();
			serverTransport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
	
}
