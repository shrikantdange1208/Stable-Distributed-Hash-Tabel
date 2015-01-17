package com.shrikant.client;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.TreeMap;

public class BigIntTest {
	public static void main(String[] args){
		BigInteger two  = BigInteger.valueOf(2);
		int maxVal = 256;
		int lastVal = 255;
		BigInteger mod = two.pow(maxVal);
		//int port  = 9091;
		String key90 = sha256("127.0.1.1:"+Integer.toString(9090));
		String key92 = sha256("127.0.1.1:"+Integer.toString(9092));
		String key91 = sha256("127.0.1.1:"+Integer.toString(9091));
		String key93 = sha256("127.0.1.1:"+Integer.toString(9093));
		String key94 = sha256("127.0.1.1:"+Integer.toString(9094));
		//TreeMap<BigInteger,Integer> nodes = new TreeMap<BigInteger,Integer>();
		BigInteger range = two.pow(maxVal).subtract(BigInteger.ONE);
		BigInteger keyVal90 = new BigInteger(key90,16);
		BigInteger keyVal92 = new BigInteger(key92,16);
		BigInteger keyVal91 = new BigInteger(key91,16);
		BigInteger keyVal94 = new BigInteger(key94,16);
		BigInteger keyVal93 = new BigInteger(key93,16);
		System.out.println("Compare 91 94:"+keyVal91.compareTo(keyVal94));
		BigInteger raiseTo = two.pow(lastVal);
		BigInteger addition = keyVal92.add(raiseTo);
		//BigInteger addition93 = keyVal93.add(raiseTo);
		int val = addition.compareTo(range);
		addition = addition.mod(mod);
		
		/*System.out.println("Addition:"+addition);
		System.out.println("Val:"+val);
		System.out.println("Compare 92 93:"+keyVal93.compareTo(addition));
		System.out.println("Compare 92 90:"+addition.compareTo(keyVal90));*/
		
		
		String tempKey = "db91bc96c9e9d6c5d88ca680a7cbf9495100e1d1eb4dc0afce7bc83c43660fe8";
		BigInteger tempVal = new BigInteger(tempKey,16);
		System.out.println("Compareeeeeeeeeee:"+tempVal.compareTo(range));
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
