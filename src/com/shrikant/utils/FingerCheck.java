package com.shrikant.utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.shrikant.service.NodeID;

public class FingerCheck {
	private List<Integer> portList;
	private String ip = null;
	private Map<Integer,FingerTable> fingerMap = null;
	
	public FingerCheck(List<Integer> ports){
		fingerMap = new HashMap<Integer,FingerTable>();
		portList = new ArrayList<Integer>();
		this.portList= ports;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
	}
	
	public List<Integer> getPortList() {
		return portList;
	}

	public void setPortList(List<Integer> portList) {
		this.portList = portList;
	}
	
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Map<Integer,FingerTable> createFingerTables(){
		NodeID startNode = null;
		int startPort = 0;
		int thisPort = 0;
		NodeID thisNode = null;
		String key = null;
		List<NodeID> fingerTable = new ArrayList<NodeID>();
		FingerTable finger = null;
		
		int index = 0;
		if(portList.size()>0){
			startPort = portList.get(0);
			startNode = new NodeID();
			startNode.setPort(startPort);
			startNode.setIp(ip);
			key = sha256(ip+":"+Integer.toString(startPort));
			startNode.setId(key);
			finger = new FingerTable();
			finger.setIndex(startNode.getPort());
			finger.setPredecessor(startNode);
			finger.setSuccessor(startNode);
			
			
			for(index = 0; index<256;index++){
				fingerTable.add(startNode);
			}
			finger.setFingers(fingerTable);
			fingerMap.put(startPort, finger);
		}
		else{
			System.out.println("Port List is NUll");
		}
		
		for(int i = 1;i<portList.size();i++){
			thisNode = new NodeID();
			thisPort = portList.get(i);
			thisNode.setPort(thisPort);
			key = sha256(ip+":"+Integer.toString(thisPort));
			thisNode.setId(key);
			thisNode.setIp(ip);
			join(startNode,thisNode);
			updateSuccessor(fingerMap);
			updatePredecessor(fingerMap);
		}
		
		
		
		return fingerMap;
	}
	public void updateSuccessor(Map<Integer, FingerTable> fingerTables) {
		
		int i;
		FingerTable currFinger = null;
		NodeID succNode = null;
		List<NodeID> fingerList = null;
		int port=0;
		for(i=0;i<fingerTables.size();i++){
			port = portList.get(i);
			currFinger = fingerTables.get(port);
			fingerList = currFinger.getFingers();
			succNode = fingerList.get(0);
			currFinger.setSuccessor(succNode);
			fingerMap.put(port, currFinger);
		}
	}
	public void updatePredecessor(Map<Integer, FingerTable> fingerTables) {
		
		int i;
		FingerTable currFinger = null;
		NodeID succNode = null;
		NodeID currNode = null;
		FingerTable succFinger = null;
		List<NodeID> fingerList = null;
		String currKey=null;
		int port=0;
		for(i=0;i<fingerTables.size();i++){
			port = portList.get(i);
			currFinger = fingerTables.get(port);
			fingerList = currFinger.getFingers();
			succNode = fingerList.get(0);
			if(succNode.getPort()==port){
				break;
			}
			currNode = new NodeID();
			currNode = currNode.setPort(port);
			currNode.setIp(ip);
			currKey = sha256(ip+":"+Integer.toString(port));
			currNode.setId(currKey);
			
			succFinger = fingerTables.get(succNode.getPort());
			succFinger.setPredecessor(currNode);
			fingerMap.put(succFinger.getIndex(), succFinger);
		}
	}

	public void join(NodeID startNode, NodeID thisNode) {
		int i;
		
		List<NodeID> newfingerTable = new ArrayList<NodeID>();
		String thisKey = thisNode.getId(); //node_new key
		BigInteger keyVal = null;
		BigInteger tempKey = null;
		BigInteger addition =null;
		
		BigInteger two = BigInteger.valueOf(2);
		BigInteger highestVal = two.pow(256);
		BigInteger range = highestVal.subtract(BigInteger.ONE);
		String nextKey=null;
		NodeID successorNode;
		NodeID predNode = null;
		keyVal = new BigInteger(thisKey,16);
		
		for(i=0;i<256;i++){
			nextKey=null;
			tempKey = two.pow(i);
			addition = keyVal.add(tempKey);
			if(addition.compareTo(range)>0){
				addition = addition.mod(highestVal);
			}
			//addition = addition.mod(highestVal);
			nextKey = addition.toString(16);
			successorNode=findSucc(nextKey,startNode);
			newfingerTable.add(i,successorNode);		
		}
		FingerTable newFinger = new FingerTable();
		newFinger.setFingers(newfingerTable);
		newFinger.setIndex(thisNode.getPort());
		
		predNode = findPred(thisNode.getId(),startNode);
		newFinger.setPredecessor(predNode);
		fingerMap.put(thisNode.getPort(), newFinger);
		
		
		/*FingerTable succFinger = fingerMap.get(successorNode.getPort());
		succFinger.setSuccessor(thisNode);
		fingerMap.put(successorNode.getPort(), succFinger);*/
		
		
		updateFingerTable(thisNode);
		
	}

	public void updateFingerTable(NodeID thisNode) {
		int i;
		String thisKey = thisNode.getId(); //node_new key i.e. key of current node
		
		BigInteger newKey = new BigInteger(thisKey,16);
		BigInteger two = BigInteger.valueOf(2);
		BigInteger lastVal = two.pow(256); //For modular addition
		BigInteger tempKey = null;
		BigInteger substraction = null;
		
		NodeID checkSuccessor=null; //Used if the (node_new-2^i is the actual node)
		NodeID predNode = null;   //Calculated predecessor node
		
		for(i=0;i<256;i++){
			tempKey = two.pow(i);
			substraction = newKey.subtract(tempKey);
			if(substraction.compareTo(BigInteger.ZERO)<0){
				substraction = substraction.add(lastVal);
			}
			String searchKey = substraction.toString(16);
			predNode = findPred(searchKey,thisNode);
			checkSuccessor = getNodeSucc(predNode);
			if(keyCompareTo(checkSuccessor.getId(),searchKey)!=0){
				updateFingerComparison(predNode,i,thisNode);
			}
			else if(keyCompareTo(checkSuccessor.getId(),searchKey)==0){
				updateFingerComparison(checkSuccessor,i,thisNode);
			}
		}
	}

	public void updateFingerComparison(NodeID checkNode, int index, NodeID thisNode) {
		String thisKey = thisNode.getId(); //node_new key i.e. key of current node
		FingerTable thisFinger = fingerMap.get(thisNode.getPort());
		List<NodeID> thisList = thisFinger.getFingers();
		NodeID predecessor = thisFinger.getPredecessor();
		String predKey = predecessor.getId(); // predecessor key of current node
		BigInteger two = BigInteger.valueOf(2);
		BigInteger tempKey = two.pow(index);
		BigInteger keyVal = null;
		String checkKey=null;
		int greaterThan=0,lessThan=0;
		NodeID predecessorNode = null;
		int interval=0;
		int flag = 0;
		BigInteger highestVal = two.pow(256);
		BigInteger range = highestVal.subtract(BigInteger.ONE);
		if(checkNode.getPort()==thisNode.getPort()){	
			keyVal = new BigInteger(checkNode.getId(),16);
			keyVal = keyVal.add(tempKey);
			if(keyVal.compareTo(range)>0){
				keyVal = keyVal.mod(highestVal);		
			}
			checkKey = keyVal.toString(16); // p+2^i	
			flag=0;
			interval = keyCompareTo(thisKey,predKey);
			if(interval>0){
				greaterThan = keyCompareTo(checkKey,predKey);
				lessThan = keyCompareTo(thisKey,checkKey);
				if((greaterThan>0)&&(lessThan>=0)){
					flag=1;
				}
			}else if(interval<0){
				greaterThan = keyCompareTo(checkKey,predKey);
				lessThan = keyCompareTo(checkKey,thisKey);
				if((greaterThan>=0)||lessThan<=0){
					flag=1;
				}
			}
				
			if(flag==1){
				thisList.set(index, thisNode);
				thisFinger.setFingers(thisList);
				fingerMap.put(thisNode.getPort(), thisFinger);
			}	
		}
		else{	
				keyVal = new BigInteger(checkNode.getId(),16);
				keyVal = keyVal.add(tempKey);
				if(keyVal.compareTo(range)>0){
					keyVal = keyVal.mod(highestVal);		
				}
				checkKey = keyVal.toString(16);
				flag=0;
				interval = keyCompareTo(thisKey,predKey);
				if(interval>0){
					greaterThan = keyCompareTo(checkKey,predKey);
					lessThan = keyCompareTo(thisKey,checkKey);
					if((greaterThan>0)&&(lessThan>=0)){
						flag=1;
					}
						
				}else if(interval<0){
					greaterThan = keyCompareTo(checkKey,predKey);
					lessThan = keyCompareTo(checkKey,thisKey);
					if((greaterThan>=0)||lessThan<=0){
						flag=1;
					}
				}
				if(flag==1){
					updateFinger(index, thisNode,checkNode);
					predecessorNode=findPred(checkNode.getId(),thisNode);
					updateFingerComparison(predecessorNode,index,thisNode);
				}
					
		}

		
	}

	public void updateFinger(int index, NodeID thisNode, NodeID checkNode) {
		
		FingerTable checkFinger = fingerMap.get(checkNode.getPort());
		List<NodeID> checkList = checkFinger.getFingers();
		checkList.set(index, thisNode);
		checkFinger.setFingers(checkList);
		fingerMap.put(checkNode.getPort(), checkFinger);
		
	}

	public NodeID findSucc(String nextKey,NodeID startNode) {
		String currentNodeKey;
		NodeID successorNode=null,predecessorNode=null;
		String currentIp=null;
		try {
			 currentIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currentNodeKey = startNode.getId();
		
		//check if the current node is the successor of the specified key
		//If yes, return this node else call findPred(key)
		//System.out.println(currentNodeKey);
		//System.out.println(nextKey);
		int result=keyCompareTo(currentNodeKey,nextKey);
		if(result==0){
			successorNode = new NodeID();
			successorNode.setId(currentNodeKey);
			successorNode.setIp(currentIp);
			successorNode.setPort(startNode.getPort());
			
		}else{
			
				predecessorNode = findPred(nextKey,startNode);
				if(predecessorNode==null){
					System.out.println("Predecessor is Null..In findSucc:");
					System.exit(0);
				}
				if(predecessorNode!=null){
				int pPort = predecessorNode.getPort();
				if(pPort==startNode.getPort()){
					successorNode=getNodeSucc(startNode);
				}
				else{
					FingerTable tempFinger = fingerMap.get(predecessorNode.getPort());
					List<NodeID> tempList = tempFinger.getFingers();
					successorNode = tempList.get(0);
				}	
			}
		}		 
		return successorNode;
		
	}

	public NodeID getNodeSucc(NodeID startNode) {
		FingerTable currFingerTable = fingerMap.get(startNode.getPort());
		List<NodeID> currFinger = currFingerTable.getFingers();
		NodeID succNode = currFinger.get(0);
		return succNode;
	}

	public NodeID findPred(String nextKey, NodeID startNode) {
		String secondKey=null,currentNodeKey=null;
		NodeID predNode=null,targetNode=null;
		int result,value,tempResult,lessValue=0,greaterValue=0;
		
		currentNodeKey = startNode.getId();
		FingerTable currFingerTable = fingerMap.get(startNode.getPort());
		List<NodeID> currFinger = currFingerTable.getFingers();
		
		NodeID currSucc = null;
		currSucc = currFinger.get(0);
		secondKey = currSucc.getId();
		
		result=keyCompareTo(currentNodeKey,secondKey);
		
		if(result>0){
			
			value=keyCompareTo(nextKey,secondKey);
			lessValue=keyCompareTo(nextKey,secondKey);
			greaterValue=keyCompareTo(nextKey,currentNodeKey);
			if(lessValue<=0||greaterValue>=0){
				predNode=startNode; 
			}
			else{
				targetNode = comparefingerTable(nextKey,startNode);
				predNode = findPred(nextKey,targetNode);
			}
		}
		else if(result==0){
			predNode=startNode;
			return predNode;
		}else if(result<0){
			
			value=keyCompareTo(nextKey,currentNodeKey);
			
			if(value>0){
				tempResult = keyCompareTo(nextKey,secondKey);
				if(tempResult<=0){
					predNode = startNode;
				}
				else{
					targetNode = comparefingerTable(nextKey,startNode);
					predNode = findPred(nextKey,targetNode);
					//RPC on targetNode
				}
				
			}
			else{
				targetNode = comparefingerTable(nextKey,startNode);
				predNode = findPred(nextKey,targetNode);
				//RPC on targetNode
			}
		}
		return predNode;
	}

	public NodeID comparefingerTable(String nextKey,NodeID startNode) {
		
		NodeID firstNode,secondNode,targetNode=null;
		String firstKey,secondKey;
		int result,value,tempResult,lessValue,greaterValue;
		int firstCounter=0;
		int secondCounter=1;
		int flag=1;
		
		//String currentNodeKey = startNode.getId();
		FingerTable currFingerTable = fingerMap.get(startNode.getPort());
		List<NodeID> currFinger = currFingerTable.getFingers();
		
		while(flag==1){
			firstNode = currFinger.get(firstCounter);
			firstKey = firstNode.getId();
			secondNode = currFinger.get(secondCounter);
			secondKey = secondNode.getId();
			
			result=keyCompareTo(firstKey,secondKey);
			if(result>0){
				value=keyCompareTo(nextKey,secondKey);
				lessValue=keyCompareTo(nextKey,secondKey);
				greaterValue=keyCompareTo(nextKey,firstKey);
				if(lessValue<=0||greaterValue>=0){
					targetNode = firstNode;
					break;
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=currFinger.size()){
						flag=0;
						targetNode = currFinger.get(currFinger.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			else if(result<0){
				value=keyCompareTo(nextKey,firstKey);
				if(value>0){
					tempResult = keyCompareTo(nextKey,secondKey);
					if(tempResult<=0){
						targetNode = firstNode;
						//return firstNode;
						break;
					}
					else{
						firstCounter++;
						secondCounter++;
						if(secondCounter>=currFinger.size()){
							flag=0;
							targetNode = currFinger.get(currFinger.size()-1);
							break;
						}
					}
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=currFinger.size()){
						flag=0;
						targetNode = currFinger.get(currFinger.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			else if(result==0){
				firstCounter++;
				secondCounter++;
				if(secondCounter>=currFinger.size()){
					flag=0;
					targetNode = currFinger.get(currFinger.size()-1);
					//return targetNode;
					break;
				}
			}
		}
		
		return targetNode;
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

	int keyCompareTo(String key1,String key2){
		int result = 0;
		BigInteger num1 = new BigInteger(key1,16);
		BigInteger num2 = new BigInteger(key2,16);
		result = num1.compareTo(num2);
		return result;
	}
}
