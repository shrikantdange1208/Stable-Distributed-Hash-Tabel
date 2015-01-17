package com.shrikant.utils;

//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.shrikant.service.DHTNode;
//import com.shrikant.service.NodeID;
import com.shrikant.service.SystemException;

public class Stabilize implements Runnable{
	
	private int port=0;
	String ip = null;
	private String threadName = null;
	private Thread nodeThread = null;
	private int interval;
	
	public Stabilize(int port, int interval){
		this.port = port;
		this.interval = interval;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		this.threadName = Integer.toString(port);
		
	}
	public  void start(){
		System.out.println("Starting thread:"+threadName);
		if(nodeThread == null){
			nodeThread = new Thread(this,threadName);
			nodeThread.start();
		}
		
	}

	@Override
	public void run() {
		//List<NodeID> fingerTable = new ArrayList<NodeID>();
		TTransport transport = null;
		TProtocol protocol = null;
		DHTNode.Client client = null;
		try {
			transport = new TSocket(ip, port);
			//transport = new TFramedTransport(new TSocket(ip, port));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			client = new DHTNode.Client(protocol);
			int flag = 1;
			//TimeUnit.SECONDS.sleep(4);
			while(flag==1){
				client.stabilize();
				TimeUnit.SECONDS.sleep(interval);
				client.fixFingers();
			}	
			//System.out.println("Stabilize Complete");
			/*fingerTable=client.getFingertable();
			writeFile(Integer.toString(port),fingerTable);*/
			transport.close();
			//System.out.println("Transport closed");
		} catch (SystemException e) {
				e.printStackTrace();
		} catch (TException e) {
				e.printStackTrace();
		} catch (InterruptedException e) {
				e.printStackTrace();
		}
			
		
		
		
		
	}
	
	
	/*public void writeFile(String fileName,List<NodeID> fingerTable){
		FileWriter writer=null;
		BufferedWriter buffWriter=null;
		NodeID tempNode = null;
		File file = new File(fileName+".txt");
		if(file.exists()){
			file.delete();
		}
		try {
				file.createNewFile();
				writer = new FileWriter(file.getName());
				buffWriter = new BufferedWriter(writer);
				System.out.println(fingerTable.size());
				buffWriter.write("Finger Table for 127.0.1.1 "+fileName);
				buffWriter.newLine();
				buffWriter.write("index     "+"ip address   "+"port    "+"digest");
				buffWriter.newLine();
				buffWriter.flush();
				for(int j=0;j<256;j++){
					tempNode = fingerTable.get(j);
					System.out.println(j+"        "+tempNode.getIp()+"    "+tempNode.getPort()+"    "+tempNode.getId());
					buffWriter.write(j+"        "+tempNode.getIp()+"    "+tempNode.getPort()+"    "+tempNode.getId());
					buffWriter.newLine();
				} 
				buffWriter.newLine();
				buffWriter.flush();
				buffWriter.close();
		} catch (IOException e) {
				e.printStackTrace();
		}
	}*/
}
