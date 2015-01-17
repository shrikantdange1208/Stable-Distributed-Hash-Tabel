package com.shrikant.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

public class ConcurrentJoin {
	public static List<NodeID> currentNodes = new ArrayList<NodeID>();
	public static List<NodeID> fingerTable = new ArrayList<NodeID>();

	public void join(int startPort, int joinPort){
		String ip=null;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		String startKey=sha256(ip+":"+Integer.toString(startPort));
		NodeID helperNode = new NodeID(startKey,ip,startPort);
		
		TTransport transport;
		
		 try {
			transport  = new TSocket(ip,joinPort);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			DHTNode.Client client = new DHTNode.Client(protocol);
			client.join(helperNode);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (TException e) {
			e.printStackTrace();
			System.exit(0);
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
