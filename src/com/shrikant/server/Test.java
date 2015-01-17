package com.shrikant.server;

import com.shrikant.server.TestServer;
import com.shrikant.service.DHTNode;
import com.shrikant.service.NodeID;
import com.shrikant.service.SystemException;
import com.shrikant.utils.ConcurrentJoin;
import com.shrikant.utils.FingerCheck;
import com.shrikant.utils.FingerTable;

/*import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
*/import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class Test {
	
	
	public static void main(String[] args){
		
		if((args[0].length()<1) || (args[1].length()<1) || (args[2].length()<1)){
			System.out.println("Please Enter valid arguments.....Exiting....");
			System.exit(1);
		}
		
		Date date = null;
		Map<Integer,FingerTable> compareFingers = null;
		List<Integer> portList = null;
		FingerCheck fingerCheck = null;
		int startPort = Integer.parseInt(args[0]);
		int intervalR = Integer.parseInt(args[1]);
		int noOfServers = Integer.parseInt(args[2]);
		
		portList = new ArrayList<Integer>();
		for(int i = 0; i<noOfServers; i++){
			portList.add(startPort+i);
		}
		compareFingers = new HashMap<Integer,FingerTable>();
		fingerCheck = new FingerCheck(portList);
		compareFingers = fingerCheck.createFingerTables();
		//writeToFile(compareFingers);
		TestServer server = null;
		
		int tempPort = 0;
		for(int i = 0;i<noOfServers;i++){
			server  = new TestServer(startPort+i,intervalR);
			server.startServer();
			tempPort = startPort+i;
			System.out.println("Started server:"+tempPort);
		}
		
		try {
			//Wait till all servers start
			System.out.println("Wait till all servers start.......");
			TimeUnit.SECONDS.sleep(2);
			ConcurrentJoin concurrJoin = new ConcurrentJoin();
			
			for(int i = 1;i<noOfServers;i++){
				date = new Date();
				System.out.println("Thrift:"+date+" TServerSocket::listen() BIND "+startPort+i);
				concurrJoin.join(startPort,startPort+i);
			}
			TimeUnit.MILLISECONDS.sleep(700);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		testResults(noOfServers,portList,compareFingers);
				
	}

	private static void testResults(int noOfServers, List<Integer> portList,
			Map<Integer, FingerTable> compareFingers) {
		
		int portNumber = 0;
		TTransport transport = null;
		TProtocol protocol = null;
		DHTNode.Client client = null;
	
		Map<Integer, FingerTable> currentMap = new HashMap<Integer,FingerTable>();
		FingerTable currentNode = null;
		List<NodeID> currFingerList = null;
		NodeID succNode = null;
		NodeID predNode = null;
		NodeID optionalSuccNode = null;
		String key = null;
		String ip=null;
		String port=null;
		int totalFindPred = 0;
		
		Map<Integer,NodeID> nodes = null;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int flag =1;
		while(flag==1){
			
			totalFindPred = 0;
			//totalCorrFindPred = 0;
			nodes = null;
			nodes = new HashMap<Integer,NodeID>();
			for(int i = 0;i<noOfServers;i++){
				
				try {
					portNumber = portList.get(i);
					//System.out.println("Connecting to port:"+portNumber);
					
					transport = new TSocket(ip,portNumber);
					protocol = new TBinaryProtocol(transport);
					transport.open();
					client = new DHTNode.Client(protocol);
					currFingerList = client.getFingertable();
					predNode = client.getNodePred();
					succNode= client.getNodeSucc();
					port = Integer.toString(portNumber);
					key = sha256(ip+":"+port);
					
					//For Extra Credit
					optionalSuccNode = client.findSucc(key);
					nodes.put(portNumber,optionalSuccNode);
					totalFindPred = totalFindPred+optionalSuccNode.getCount();
					//For Extra Credit
					
					
					currentNode= new FingerTable();
					currentNode.setFingers(currFingerList);
					currentNode.setIndex(portNumber);
					currentNode.setPredecessor(predNode);
					currentNode.setSuccessor(succNode);
					currentMap.put(portNumber, currentNode);
					//System.out.println("got fingertable for port:"+(portNumber));
					transport.close();
					
					
				} catch (TTransportException e) {
					e.printStackTrace();
				} catch (SystemException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				} 
			}
			computeResults(currentMap,compareFingers,portList,totalFindPred,nodes);
			try {
				TimeUnit.MILLISECONDS.sleep(700);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
	}

	private static void computeResults(Map<Integer, FingerTable> currentMap,
			Map<Integer, FingerTable> compareFingers, List<Integer> portList, int totalFindPred, Map<Integer,NodeID> nodes) {
		Date date = null;
		int noOfNodes = portList.size();
		float avgCorrSucc=0;
		float avgFindPred = 0;
		

		int numberOfCorrectSucc = 0;
		int numberOfInCorrSucc = 0;
		float percentFailedSucc = 0;
		int numberOfCorrectPred = 0;
		int numberOfCorrectEntries = 0;
		int numberOfInCorrectEntries= 0;
		int totalCorrFindPred = 0;
		NodeID checkSuccNode = null;
		NodeID checkPredNode = null;
		NodeID correctSuccNode = null;
		NodeID correctPredNode = null;
		int correct = 0;
		FingerTable checkNode = null; // current finger to compute results
		FingerTable correctNode = null; //correct finger table
		List<Integer> fingerEntries = new ArrayList<Integer>();
		List<NodeID> currentFingerTable = null;
		List<NodeID> correctFingerTable = null;
		int i=0;
		for(i=0;i<currentMap.size();i++){
			checkNode = currentMap.get(portList.get(i));
			correctNode = compareFingers.get(portList.get(i));
			
			checkSuccNode = checkNode.getSuccessor();
			correctSuccNode = correctNode.getSuccessor();
			//Check Successor node
			correct = checkSuccessor(checkSuccNode,correctSuccNode);
			//If same increment number of correct successors
			if(correct==1){
				numberOfCorrectSucc++;
				totalCorrFindPred = totalCorrFindPred + getFindPredCount(checkSuccNode.getPort(),nodes);
				
			}
			correct=0;
			checkPredNode = checkNode.getPredecessor();
			correctPredNode = correctNode.getPredecessor();
			correct = checkPredecessor(checkPredNode,correctPredNode);
			//If same increment number of correct predecessors
			if(correct==1){
				numberOfCorrectPred++;
			}
			
			currentFingerTable = checkNode.getFingers();
			correctFingerTable = correctNode.getFingers();
			fingerEntries = checkFingerTable(currentFingerTable,correctFingerTable);
			numberOfCorrectEntries = numberOfCorrectEntries+fingerEntries.get(0);
			numberOfInCorrectEntries = numberOfInCorrectEntries + fingerEntries.get(1);
			
		}
		date = new Date();
		System.out.println("Thrif:"+date);
		System.out.println("Correct Successors:"+numberOfCorrectSucc);
		System.out.println("Correct Predecessors:"+numberOfCorrectPred);
		System.out.println("Correct Finger Entries:"+numberOfCorrectEntries);
		System.out.println("InCorrect Finger Entries:"+numberOfInCorrectEntries);
		System.out.println("EXTRA CREDIT:");
		//OPTIONAL PART
		
		avgCorrSucc =  (float)numberOfCorrectSucc/noOfNodes;
		System.out.println("Avg Correct Successors:"+avgCorrSucc);
		avgFindPred = (float)totalFindPred/noOfNodes;
		System.out.println("Avg number of Calls to total find pred for each correct Successor:"+avgFindPred);
		//float tempAvg = (float)totalCorrFindPred/noOfNodes;
		numberOfInCorrSucc = noOfNodes - numberOfCorrectSucc;
		percentFailedSucc = (float)numberOfInCorrSucc/noOfNodes;
		percentFailedSucc = percentFailedSucc*100;
		System.out.println("Percentage(%) of failed calls to findSucc:"+percentFailedSucc);
		System.out.println("\n");
		
		//System.out.println("Total Correct FInd pred:"+totalCorrFindPred);
		//System.out.println("Total Find Pred:"+totalFindPred);
		//System.out.println("Avg number of Calls to find pred for each correct Successor:"+avgFindPred);
		
		
		
	}

	private static int getFindPredCount(int port, Map<Integer,NodeID> nodes) {
		int count = 0;
		NodeID tempNode = nodes.get(port);
		count = tempNode.getCount();
		return count;
	}


	private static List<Integer> checkFingerTable(
			List<NodeID> currentFingerTable, List<NodeID> correctFingerTable) {
		int i=0;
		int inCorrectEntries =0;
		int correctEntries = 0;
		NodeID currentNode = null;
		NodeID correctNode = null;
		List<Integer> fingerEntries = new ArrayList<Integer>();
		int currentPort = 0;
		int correctPort = 0;
		
		for(i=0;i<currentFingerTable.size();i++){
			
			currentNode = currentFingerTable.get(i);
			currentPort = currentNode.getPort();
			correctNode = correctFingerTable.get(i);
			correctPort = correctNode.getPort();
			
			if((currentPort!=0)&& (currentPort==correctPort)){
				correctEntries++;
			}
			else if((currentPort!=0)&& (currentPort!=correctPort)){
				inCorrectEntries++;
			}
		}
		fingerEntries.add(correctEntries);
		fingerEntries.add(inCorrectEntries);
		
		return fingerEntries;
	}

	private static int checkSuccessor(NodeID checkSuccNode,
			NodeID correctSuccNode) {
		
		if(checkSuccNode.getPort()==correctSuccNode.getPort()){
			return 1;
		}
		return 0;
	}
	private static int checkPredecessor(NodeID checkPredNode,
			NodeID correctPredNode) {
		
		if(checkPredNode.getPort()==correctPredNode.getPort()){
			return 1;
		}
		return 0;
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
	
	/*public static void writeToFile(Map<Integer,FingerTable> fingerMap){
		FileWriter writer=null;
		BufferedWriter buffWriter=null;
		Set<Map.Entry<Integer, FingerTable>> entry  = fingerMap.entrySet();
		int port = 0;
		List<NodeID> fingerTable = null;
		FingerTable tempNode = null;
		NodeID temp=null;
		File file = new File("fingers.txt");
		if(file.exists()){
			file.delete();
		}
		try {
			file.createNewFile();
			writer= new FileWriter(file.getName());
			buffWriter = new BufferedWriter(writer);
			buffWriter.flush();
			for(Map.Entry<Integer, FingerTable> node: entry){
				port = node.getKey();
				tempNode = node.getValue();
				buffWriter.write("Finger Table for 127.0.1.1 "+port);
				buffWriter.newLine();
				buffWriter.write("index     "+"ip address   "+"port    "+"digest");
				buffWriter.newLine();
				fingerTable = tempNode.getFingers();
				for(int j=0;j<256;j++){
					temp = fingerTable.get(j);
					buffWriter.write(j+"        "+temp.getIp()+"    "+temp.getPort()+"    "+temp.getId());
					buffWriter.newLine();
				}
			}
			buffWriter.flush();
		} catch (IOException e) {
				e.printStackTrace();
		}
		
	}*/
}
