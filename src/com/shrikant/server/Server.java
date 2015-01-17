package com.shrikant.server;

//import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
//import org.apache.thrift.transport.TNonblockingServerSocket;
//import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.handler.FileHandler;
import com.shrikant.service.DHTNode;
import com.shrikant.service.DHTNode.Iface;
import com.shrikant.utils.Stabilize;

public class Server {
	
	private static FileHandler fileHandler;
	private static DHTNode.Processor<Iface> processor;
	public static void main(String args[]){
		
		if((args[0].length()<1) || (args[1].length()<1)){
			System.out.println("Please Enter valid arguments.....Exiting....");
			System.exit(1);
		}
		int portNumber = Integer.parseInt(args[0].trim());
		int interval = Integer.parseInt(args[1]);
		System.out.println("PortNumber");
		fileHandler = new FileHandler(portNumber);
		processor = new DHTNode.Processor<DHTNode.Iface>(fileHandler);
		startServer(processor,portNumber,interval);
		
	}

	private static void startServer(DHTNode.Processor<com.shrikant.service.DHTNode.Iface> processor,int portNumber,int interval) {
		try {
			
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			//TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(portNumber);
			//TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
			
			//TServer server = new TThreadedServer()
			Stabilize stabilize= new Stabilize(portNumber,interval);
			stabilize.start();
			System.out.println("Starting the file server");
			server.serve();
			serverTransport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		
	}
}
