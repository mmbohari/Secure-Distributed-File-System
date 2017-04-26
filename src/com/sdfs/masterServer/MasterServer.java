package com.sdfs.masterServer;

import java.util.Scanner;

/**
 * 
 * This class represents Master Server. Master Server listens for 
 * requests on listeningPort.
 * 
 * @author mmbohari
 *
 */
public class MasterServer {

	static int listeningPort;
	Scanner sc;
	
	/*
	 * This method is used to take port number that the Master Server
	 * will listen connection on
	 */
	public int inputListeningPort(){
		sc = new Scanner(System.in);
		
		System.out.println("Enter listening port for Master Server");
		try {
			listeningPort = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected for listening"
					+ " port (>1024 && 64k<)");
		}
		return listeningPort;
	}
	
	/*
	 * This method initializes Master Server i.e. takes listening port
	 */
	public void initialise(){
		displayLines();
		System.out.println("Initialising master");
		
		// take listening port
		do {
			listeningPort = inputListeningPort();
		} while (listeningPort == 0);
		System.out.println("Master Server will listen for connections"
				+ " on port: " + listeningPort);
		displayLines();
	}
	
	/*
	 * This method is used to get listeningPort
	 */
	public int getListeningPort(){
		return listeningPort;
	}
	
	/*
	 * This method is used to display lines on console 
	 * for making the screen elegant 
	 */
	public void displayLines(){
		System.out.println("=================================>>");
	}
	
}
