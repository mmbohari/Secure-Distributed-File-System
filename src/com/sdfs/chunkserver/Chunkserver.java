package com.sdfs.chunkserver;

import java.util.Scanner;

/**
 * This class represents Chunkservers.
 * 
 * @author mmbohari
 *
 */
public class Chunkserver {

	static int listeningPort;
	static String chunkServerName;
	Scanner sc;
	
	/*
	 * Constructor of Chunkserver
	 */
	public Chunkserver(){
		chunkServerName = "Chunkserver";
	}
	
	/*
	 * This method is used to take port number that the Chunkserver
	 * listens for connections
	 */
	public int inputListeningPort(){
		sc = new Scanner(System.in);
		
		System.out.println("Enter listening port for " + chunkServerName);
		try {
			listeningPort = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected for listening"
					+ " port (>1024 && 64k<)");
		}
		return listeningPort;
	}
	
	/*
	 * This method is used to assign a chunkserver number to the
	 * current chunkserver
	 */
	public int inputChunkserverNumber(){
		sc = new Scanner(System.in);
		int number = 0;
		
		System.out.println("Enter a number for the chunkserver");
		try {
			number = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected for chunk"
					+ "server number");
		}
		
		return number;
	}
	
	/*
	 * This method is used to initialize chunkservers
	 */
	public void initialise(){
		displayLines();
		System.out.println("Initialising chunkserver");
		// input chunkserver number
		chunkServerName += inputChunkserverNumber();
		// input listening port
		do {
			listeningPort = inputListeningPort();
		} while (listeningPort == 0);
		System.out.println( chunkServerName + " will listen for connections"
				+ " on port: " + listeningPort);
		displayLines();
	}
	
	/*
	 * This method is used to display lines on console 
	 * for making the screen elegant 
	 */
	public void displayLines(){
		System.out.println("=================================>>");
	}
	
}
