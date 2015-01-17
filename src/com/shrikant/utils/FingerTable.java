package com.shrikant.utils;

import java.util.ArrayList;
import java.util.List;

import com.shrikant.service.NodeID;

public class FingerTable {
	private int port;
	private NodeID predecessor;
	private NodeID successor;
	private List<NodeID> fingers;
	
	public FingerTable(){
		fingers = new ArrayList<NodeID>();
	}
	public int getIndex() {
		return port;
	}
	public void setIndex(int index) {
		this.port = index;
	}
	public NodeID getPredecessor() {
		return predecessor;
	}
	public void setPredecessor(NodeID predecessor) {
		this.predecessor = predecessor;
	}
	public NodeID getSuccessor() {
		return successor;
	}
	public void setSuccessor(NodeID successor) {
		this.successor = successor;
	}
	public List<NodeID> getFingers() {
		return fingers;
	}
	public void setFingers(List<NodeID> fingers) {
		this.fingers = fingers;
	}
	
	
}
