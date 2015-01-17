package com.shrikant.handler;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.service.DHTNode;
import com.shrikant.service.NodeID;
import com.shrikant.service.SystemException;
public class FileHandler implements DHTNode.Iface{
	
	private List<NodeID> fingerTable;
	int portNumber;
	String deleteFile;
	NodeID thisNode=null;
	NodeID predecessor=null;
	private Random random = null;
	private int minimum;
	private int maximum;
	private int range;
	public FileHandler(int port){
		fingerTable = new CopyOnWriteArrayList<NodeID>();
		portNumber=port;
		String tempPort = Integer.toString(port);
		
		String currentNodeKey;
		String currentIp=null;
		try {
			currentIp = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		currentNodeKey = sha256(currentIp+":"+tempPort);
		
		thisNode = new NodeID(currentNodeKey,currentIp,port);
		NodeID tempNode = new NodeID();
		tempNode.setIp(currentIp);
		tempNode.setId(currentNodeKey);
		tempNode.setPort(0);
		//Add thisNode as the first entry of its finger table
		//All other entries should be set to tempNode with port 0.
		fingerTable.add(0,thisNode);
		for(int i=1;i<256;i++){
			fingerTable.add(i,tempNode);
		}
		// Set predecessor to null
		predecessor = null;
		
		random  = new Random();
		minimum = 1;
		maximum = 255;
		range = (maximum -minimum)+1;
		
	}

	@Override
	public NodeID findSucc(String key) throws SystemException, TException {
		
		//Call findPred on this node for the specified key
		TTransport transport = null;
		TProtocol protocol = null;
		NodeID successorNode=null,predecessorNode=null;
			try{
				predecessorNode = findPred(key);
				
				if(predecessorNode==null){
					SystemException nullException = new SystemException();
					nullException.setMessage("Node cannot be found");
					throw nullException;
				}
			}catch(SystemException exception){
				throw exception;
			}
			
			if(predecessorNode!=null){
				
				String host = predecessorNode.getIp();
				int pPort = predecessorNode.getPort();
				if(pPort==portNumber){
					successorNode=this.getNodeSucc();
				}
				else{
					try {
						transport  = new TSocket(host, pPort);
						transport.open();
						protocol = new TBinaryProtocol(transport);
						DHTNode.Client client = new DHTNode.Client(protocol);
						try {
							successorNode=client.getNodeSucc();
						} catch (SystemException e) {
							throw e;
						} catch (TException e) {
							e.printStackTrace();
							System.exit(0);
						}
						transport.close();
					} catch (TTransportException e) {
						e.printStackTrace();
						System.exit(0);
					}
				}	
			}
		successorNode.setCount(predecessorNode.getCount()+1);
		return successorNode;
	}
	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		//key.keyCompareTo(currentKey);
		//negative value if specified key is greater;
		//zero if specified key is equal;
		//positive value if specified key is smaller;
		
		NodeID predNode=null,tempNode,currentNode,targetNode;
		String currentNodeKey;
		String currentIp=null;
		try {
			currentIp = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		String port = Integer.toString(portNumber);
		currentNodeKey = sha256(currentIp+":"+port);
		
		
		String secondKey;
		int result,value,tempResult,lessValue=0,greaterValue=0;
		
		//Create current node object
		currentNode = new NodeID();
		currentNode.setId(currentNodeKey);
		currentNode.setIp(currentIp);
		currentNode.setPort(portNumber);
		//Create current node object
		
		//Check if key lies between currentNode and First entry in fingerTable
		//If satisfied, currentNode is the predecessor
		
		if(fingerTable.size()>0){
			
			tempNode = fingerTable.get(0);
			secondKey = tempNode.getId();
			
			//Check if second key is smaller than the first key to adjust order of comparison
			result=keyCompareTo(currentNodeKey,secondKey);
			
			if(result>0){
				
				value=keyCompareTo(key,secondKey);
				lessValue=keyCompareTo(key,secondKey);
				greaterValue=keyCompareTo(key,currentNodeKey);
				if(lessValue<=0||greaterValue>0){
					predNode=currentNode; 
				}
				else{
					
					targetNode = comparefingerTable(key);
					try{
						predNode = nextRpcCall(targetNode,key);
					}catch(SystemException exception){
						throw exception;
					}
				}
			}else if(result==0){
				predNode=currentNode;
				return predNode;
			}else if(result<0){
				
				value=keyCompareTo(key,currentNodeKey);
				
				if(value>0){
					tempResult = keyCompareTo(key,secondKey);
					if(tempResult<=0){
						predNode = currentNode;
					}
					else{
						targetNode = comparefingerTable(key);
						try{
							predNode = nextRpcCall(targetNode,key);
						}catch(SystemException exception){
							throw exception;
						}
						//RPC on targetNode
					}
					
				}
				else{
					targetNode = comparefingerTable(key);
					try{
						predNode = nextRpcCall(targetNode,key);
					}catch(SystemException exception){
						throw exception;
					}
					//RPC on targetNode
				}
			}
		}
		else{
			return null;
		}
		
		return predNode;
	}
	

	
private NodeID comparefingerTable(String key) {
		
		//key.keyCompareTo(currentKey);
		//negative value if specified key is greater;
		//zero if specified key is equal;
		//positive value if specified key is smaller;
		
		NodeID firstNode,secondNode,targetNode=null;
		String firstKey,secondKey;
		int result,value,tempResult,lessValue,greaterValue;
		int firstCounter=0;
		int secondCounter=1;
		int flag=1;
		while(flag==1){
			firstNode = fingerTable.get(firstCounter);
			firstKey = firstNode.getId();
			
			//Check if fingerTable entry for next node is not null
			//If it is null return firstNode
			if(fingerTable.get(secondCounter).getPort()==0){
				return firstNode;
			}
			//If not continue with normal procedure
			secondNode = fingerTable.get(secondCounter);
			secondKey = secondNode.getId();
			
			//Check if second key is smaller than the first key to adjust order of comparison
			result=keyCompareTo(firstKey,secondKey);
			if(result>0){
				
				value=keyCompareTo(key,secondKey);
				lessValue=keyCompareTo(key,secondKey);
				greaterValue=keyCompareTo(key,firstKey);
				if(lessValue<=0||greaterValue>0){
					targetNode = firstNode;
					break;
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=fingerTable.size()){
						flag=0;
						targetNode = fingerTable.get(fingerTable.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			else if(result<0){
				value=keyCompareTo(key,firstKey);
				if(value>0){
					tempResult = keyCompareTo(key,secondKey);
					if(tempResult<=0){
						targetNode = firstNode;
						//return firstNode;
						break;
					}
					else{
						firstCounter++;
						secondCounter++;
						if(secondCounter>=fingerTable.size()){
							flag=0;
							targetNode = fingerTable.get(fingerTable.size()-1);
							
							break;
						}
					}
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=fingerTable.size()){
						flag=0;
						targetNode = fingerTable.get(fingerTable.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			else if(result==0){
				firstCounter++;
				secondCounter++;
				if(secondCounter>=fingerTable.size()){
					flag=0;
					targetNode = fingerTable.get(fingerTable.size()-1);
					//return targetNode;
					break;
				}
			}
			
		}
		return targetNode;
	}

	private NodeID nextRpcCall(NodeID targetNode, String key) throws SystemException,TException{
		
		NodeID predNode=null;
		TTransport transport = null;
		TProtocol protocol = null;
		if(portNumber == targetNode.getPort()){
			try {
				SystemException  exception = new SystemException();
				exception.setMessage("Calling same port again, Last entry is same as current node.Infinit loop");			
				throw exception;
			} catch (SystemException e) {
				
				e.printStackTrace();
			}
		}
		String host = targetNode.getIp();
		int port = targetNode.getPort();
		
		try {
			transport  = new TSocket(host,port);
			//transport  = new TFramedTransport(new TSocket(host, port));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			DHTNode.Client client = new DHTNode.Client(protocol);
			try {
				predNode=client.findPred(key);
				predNode.setCount(predNode.getCount()+1);
				transport.close();
			} catch (SystemException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
			
		} catch (TTransportException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return predNode;
	}
		
	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		
		NodeID succNode = null;
		if(fingerTable.size()>0){
			succNode = fingerTable.get(0);
		}
	
		return succNode;
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
	
	
	
	
	
	/*public void setNodePred(NodeID nodeId) throws SystemException, org.apache.thrift.TException{
		predecessor = nodeId;
	}*/
	
	@Override
    public void join(NodeID nodeId) throws SystemException, org.apache.thrift.TException{
		//int i;
		//BigInteger keyVal = null;
		//BigInteger newKey = new BigInteger(thisKey,16);
		//BigInteger tempKey = null;
		//BigInteger addition =null;
		//BigInteger two = BigInteger.valueOf(2);
		//BigInteger highestVal = two.pow(256);
		//BigInteger range = highestVal.subtract(BigInteger.ONE);
		//keyVal = new BigInteger(thisKey,16);
		//String nextKey=null;
		//NodeID predNode = null;
		String ip;
		int port;
		TTransport transport=null;
		TProtocol protocol=null;
		DHTNode.Client client = null;	
		String thisKey = thisNode.getId(); //node_new key
		NodeID successorNode;
		
		String succIp = null;
		int succPort = 0;
		ip = nodeId.getIp();
		port = nodeId.getPort();
		
		
		try {	
			transport  = new TSocket(ip,port);
			//transport  = new TFramedTransport(new TSocket(ip, port));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			client = new DHTNode.Client(protocol);
			
			//Get successor of current node and set it as the first entry
			successorNode = client.findSucc(thisKey);
			fingerTable.set(0,successorNode);
			
			//Notify successor to update its predecessor
			//Check if successor is same as to whom request of join is sent
			if(nodeId.getPort()==successorNode.getPort()){
				client.notify(thisNode);
				transport.close();
			}
			else{
				//connect to the successor and notify him to change its predecessor
				transport.close();
				succIp = successorNode.getIp();
				succPort = successorNode.getPort();
				transport = new TSocket(succIp,succPort);
				//transport  = new TFramedTransport(new TSocket(succIp, succPort));
				transport.open();
				protocol = new TBinaryProtocol(transport);
				client = new DHTNode.Client(protocol);
				client.notify(thisNode);
				transport.close();
			}
		} catch(SystemException e){
			throw e;
		} catch(TException e){
			e.printStackTrace();
			System.exit(0);
		}			
    }

	//New Methods
	@Override
	public NodeID getNodePred() throws SystemException, TException {
		if(predecessor==null){
			return null;
		}
		else{
			return predecessor;
		}
		
	}

	@Override
	public void stabilize() throws SystemException, TException {
		//System.out.println("In stabilize method for port:"+thisNode.getPort());
		TTransport transport = null;
		TProtocol protocol = null;
		DHTNode.Client client = null;
		NodeID predNode =null;
		NodeID successorNode = fingerTable.get(0);
		String succIp = successorNode.getIp();
		int succPort = successorNode.getPort();
		String predIp =null;
		int predPort = 0;
		int stabilizeFlag = 0;
		
		if(successorNode.getPort()==thisNode.getPort()){
			predNode = getNodePred();
			if(predNode==null){
				notify(thisNode);
			}
			/*else if(predNode.getPort()==thisNode.getPort()){
				notify(thisNode);
			}*/
			else{
				fingerTable.set(0, predNode);
			}
		}
		else{
			transport = new TSocket(succIp,succPort);
			//transport  = new TFramedTransport(new TSocket(succIp, succPort));
			transport.open();
			protocol = new TBinaryProtocol(transport);
			client = new DHTNode.Client(protocol);
			predNode = client.getNodePred();
			if(predNode==null){
				client.notify(thisNode);
				transport.close();
			}
			else{
				stabilizeFlag = needToStabilize(successorNode,predNode);
				if(stabilizeFlag==1){
					//If predNode is between current node and its previous successor
					//Change its successor to predNode.
					//Now notify predNode that set me as your predecessor
					fingerTable.set(0, predNode);
					stabilizeFlag=0;
					transport.close();
					stabilizeFlag=0;
					predIp = predNode.getIp();
					predPort = predNode.getPort();
					transport = new TSocket(predIp,predPort);
					transport.open();
					protocol  = new TBinaryProtocol(transport);
					client = new DHTNode.Client(protocol);
					client.notify(thisNode);
					transport.close();
				}
				else{
					client.notify(thisNode);
					transport.close();
				}
			}
		}
		
		/*if((predNode==null)){
			notify(thisNode);
		}
		else if((predNode!=null)&&(predNode.getPort()==thisNode.getPort())){
			stabilizeFlag = 0;
		}
		else if((predNode.getPort()==0) && (successorNode.getPort()!=thisNode.getPort())){
			stabilizeFlag = 1;
		}
		else if((predNode!=null)){
			stabilizeFlag = needToStabilize(successorNode,predNode);
		}*/
		//Check if predecessor of successor node is other than the current node and
		//if it falls between currentNode and the successorNode
		//If yes set this predecessor node as current node's successor(first entry of finger table)
	
	}
	int needToStabilize(NodeID successorNode, NodeID predNode){
		
		//Check if predecessor of successor node is other than the current node and
		//if it falls between currentNode and the successorNode
		//If yes set this predecessor node as current node's successor(first entry of finger table) (return 1)
		
		String thisKey = null;
		String succKey = null;
		
		String predKey=null;
		int result = 0;
		int lessValue = 0;
		int greaterValue=0;
		thisKey = thisNode.getId();
		succKey = successorNode.getId();
		predKey = predNode.getId();
		
		result = thisKey.compareTo(succKey);
		if(result==0){
			return 1;
		}
		else if(result<0){
			greaterValue = predKey.compareTo(thisKey);    //predecessor key should be greater than current key.
			lessValue = predKey.compareTo(succKey);       //predecessor key should be less than or equal to successor key
			if((greaterValue>0)&& (lessValue<=0)){
				return 1;
			}
		}
		else if(result>0){
			lessValue = predKey.compareTo(succKey);  //predKey should be less than successor Key
			greaterValue = predKey.compareTo(thisKey);  //predKey should be greater than this key
			if((lessValue<=0)||(greaterValue>0)){
				return 1;
			}
		}
		return 0;
	}
	//Function replacing setNodeProcessor(NodeID nodeId)
	@Override
	public void notify(NodeID nodeId) throws SystemException, TException {
		//given a node, n , this function sets the predecessor of the current node, n, to n if n ∈ (n.predecessor, n)
		
		String currPredKey =null;
		String thisKey = null;
		String newPredKey;
		BigInteger currPredVal = null;
		BigInteger thisVal = null;
		BigInteger newPredVal=null;
		
		int lessValue, greaterValue;
		int result = 0;
		//for first node predecessor is set to null, so check if it is null
		
		if(predecessor==null){
			//System.out.println("In notify: in for node:"+thisNode.getPort());
			predecessor = nodeId;
			//Test Code
			//System.out.println("Predecessor of"+thisNode.getPort()+" updated to:"+thisNode.getPort()+" id:"+thisNode.getId());
			//Test Code
		}
		else{
			newPredKey = nodeId.getId();
			newPredVal = new BigInteger(newPredKey,16);
			currPredKey = predecessor.getId();
			currPredVal = new BigInteger(currPredKey,16);
			thisKey = thisNode.getId();
			thisVal = new BigInteger(thisKey,16);
			
			//Check if predecessor of this node is same i.e. thisNode
			result = currPredVal.compareTo(thisVal);
			if(result==0){
				predecessor = nodeId;
				//System.out.println("Predecessor of"+thisNode.getPort()+" updated to:"+predecessor.getPort()+" id:"+predecessor.getId());
			}
			else if(result>0){
				//predecessor key is greater than current key
				lessValue =  newPredVal.compareTo(thisVal);
				greaterValue = newPredVal.compareTo(currPredVal);
				if(lessValue<0 || greaterValue>0){
					predecessor = nodeId;
					//System.out.println("Predecessor of"+thisNode.getPort()+" updated to:"+predecessor.getPort()+" id:"+predecessor.getId());
				}
				//Test Code
				
				//Test Code
			}
			else if(result < 0){
				//predecessor key is less than current key
				greaterValue = newPredVal.compareTo(currPredVal);
				lessValue = newPredVal.compareTo(thisVal);
				if(lessValue<0 || greaterValue>0){
					predecessor = nodeId;
					//System.out.println("Predecessor of"+thisNode.getPort()+" updated to:"+predecessor.getPort()+" id:"+predecessor.getId());
				}
				
			}
			
		}
		
		
	}

	@Override
	public void fixFingers() throws SystemException, TException {
		int index = 0;
		index = random.nextInt(range)+minimum;
		NodeID successorNode = null;
		
		String thisKey=null;
		BigInteger thisVal = null;
		String fixKey = null;
		BigInteger nextVal = null;
		BigInteger two =BigInteger.valueOf(2);
		BigInteger raiseTo = two.pow(index);
		
		BigInteger maxVal = two.pow(256);
		BigInteger maxRange = maxVal.subtract(BigInteger.ONE);
		
		thisKey = thisNode.getId();
		thisVal = new BigInteger(thisKey,16);
		
		nextVal = thisVal.add(raiseTo);
		if(nextVal.compareTo(maxRange)>0){
			nextVal = nextVal.mod(maxRange);
		}
		fixKey = nextVal.toString(16);
		successorNode = findSucc(fixKey);
		if(successorNode!=null){
			fingerTable.set(index, successorNode);
		}
		else if(successorNode==null){
			fingerTable.set(index, null);
		}
		/*NodeID tempNode=null;
		if(fingerTable.get(index)!=null){
			tempNode = fingerTable.get(index);
			//System.out.println("Entry of index:"+index+" updated to:"+tempNode.getPort());
		}
		else{
			System.out.println("entry is null");
		}*/
	}
    //New Methods

	
	int keyCompareTo(String key1,String key2){
		int result = 0;
		BigInteger num1 = new BigInteger(key1,16);
		BigInteger num2 = new BigInteger(key2,16);
		result = num1.compareTo(num2);
		return result;
	}




	@Override
	public List<NodeID> getFingertable() throws SystemException, TException {
		
		return fingerTable;
		
		
	}




	
}