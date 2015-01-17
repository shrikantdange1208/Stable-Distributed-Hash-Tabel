package com.shrikant.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.service.DHTNode;
import com.shrikant.service.NodeID;

public class Debugger {

	public static void main(String[] args) {
		FileWriter writer=null;
		BufferedWriter buffWriter=null;
		int tempPort;
		
		List<NodeID> nodes = new ArrayList<NodeID>();
		List<NodeID>fingerTable=null;
		NodeID nodeId=null;
		NodeID tempNode=null;
		NodeID predNode = null;
		NodeID succNode = null;
		String key=null;
		String port=null;
		
		nodeId=new NodeID();
		nodeId.setIp("127.0.1.1");
		nodeId.setPort(9090);
		port = Integer.toString(9090);
		key = sha256("127.0.1.1"+":"+port);
		nodeId.setId(key);
		nodes.add(nodeId);
		
		nodeId = new NodeID();
		nodeId.setIp("127.0.1.1");
		nodeId.setPort(9091);
		port = Integer.toString(9091);
		key = sha256("127.0.1.1"+":"+port);
		nodeId.setId(key);
		nodes.add(nodeId);
		
		nodeId = new NodeID();
		nodeId.setIp("127.0.1.1");
		nodeId.setPort(9092);
		port = Integer.toString(9092);
		key = sha256("127.0.1.1"+":"+port);
		nodeId.setId(key);
		nodes.add(nodeId);
		
		/*nodeId = new NodeID();
		nodeId.setIp("127.0.1.1");
		nodeId.setPort(9093);
		port = Integer.toString(9093);
		key = sha256("127.0.1.1"+":"+port);
		nodeId.setId(key);
		nodes.add(nodeId);
		
		nodeId = new NodeID();
		nodeId.setIp("127.0.1.1");
		nodeId.setPort(9094);
		port = Integer.toString(9094);
		key = sha256("127.0.1.1"+":"+port);
		nodeId.setId(key);
		nodes.add(nodeId);*/
		
		File file = new File("debug3.txt");
		if(file.exists()){
			file.delete();
		}
		try {
				file.createNewFile();
				writer = new FileWriter(file.getName());
				buffWriter = new BufferedWriter(writer);
		} catch (IOException e) {
				e.printStackTrace();
		}
		
		System.out.println("Node:"+nodes.size());
		for(int i=0;i<nodes.size();i++){
			
			TTransport transport;
			NodeID currentNode = nodes.get(i);
			tempPort = currentNode.getPort();
			System.out.println(tempPort);
			 try {
				 buffWriter.flush();
				transport  = new TSocket("127.0.1.1",tempPort);
				//transport  = new TFramedTransport(new TSocket("127.0.1.1", tempPort));
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				DHTNode.Client client = new DHTNode.Client(protocol);
				fingerTable=client.getFingertable();
				predNode = client.getNodePred();
				succNode = client.getNodeSucc();
				transport.close();
				//System.out.println(fingerTable.size());
				buffWriter.write("Finger Table for 127.0.1.1 "+tempPort);
				buffWriter.newLine();
				System.out.println("Predecessor is:"+predNode.getPort());
				buffWriter.newLine();
				System.out.println("Successor is:"+succNode.getPort());
				buffWriter.newLine();
				buffWriter.write("index     "+"ip address   "+"port    "+"digest");
				//System.out.println("index     "+"ip address   "+"port    "+"digest");
				buffWriter.newLine();
				buffWriter.flush();
				for(int j=0;j<256;j++){
					tempNode = fingerTable.get(j);
					//System.out.println(j+"        "+tempNode.getIp()+"    "+tempNode.getPort()+"    "+tempNode.getId());
					buffWriter.write(j+"        "+tempNode.getIp()+"    "+tempNode.getPort()+"    "+tempNode.getId());
					buffWriter.newLine();
				} 
				buffWriter.newLine();
				buffWriter.newLine();
				buffWriter.flush();
			} catch (TTransportException e) {
				e.printStackTrace();
				System.exit(0);
			} catch (TException e) {
				e.printStackTrace();
				System.exit(0);
			}
			 catch (IOException e) {
					
					e.printStackTrace();
			}
		}
		try {
			buffWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static String sha256(String base) {
	    try{
	        MessageDigest digest = MessageDigest.getInstance("SHA-256");
	        byte[] hash = digest.digest(base.getBytes("UTF-8"));
	        StringBuffer hexString = new StringBuffer();

	        for (int i = 0; i < hash.length; i++) {
	            String hex = Integer.toHexString(0xff & hash[i]);
	            if(hex.length() == 1) hexString.append('0');
	            hexString.append(hex);
	        }

	        return hexString.toString();
	    } catch(Exception ex){
	       throw new RuntimeException(ex);
	    }
	}

}
