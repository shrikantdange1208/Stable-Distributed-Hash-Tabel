package com.shrikant.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.shrikant.service.NodeID;

public class TestFingerGen {
	public static void main(String[] args){
		List<Integer> ports = new ArrayList<Integer>();
		Map<Integer,FingerTable> fingerTable = new HashMap<Integer,FingerTable>();
		ports.add(3233);
		ports.add(3234);
		ports.add(3235);
		FingerCheck finger = new FingerCheck(ports);
		fingerTable = finger.createFingerTables();
		System.out.println("Done:"+fingerTable.size());
		FingerTable fingerObj = null;
		List<NodeID> nodes =null;
		for(int i=0;i<fingerTable.size();i++){
			System.out.println("FigerTable for :"+ports.get(i));
			fingerObj = fingerTable.get(ports.get(i));
			System.out.println("Predecessor:"+fingerObj.getPredecessor().getPort());
			System.out.println("Successor:"+fingerObj.getSuccessor().getPort());
			nodes = fingerObj.getFingers();
			for(int j=0;j<256;j++){
				System.out.println("Port:"+nodes.get(j).getPort());
			}
		}
	}
}
