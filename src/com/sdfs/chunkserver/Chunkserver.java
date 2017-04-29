package com.sdfs.chunkserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

/**
 * This class represents Chunkservers.
 * 
 * @author mmbohari
 *
 */
public class Chunkserver {

	private static int listeningPort;
	private static int listeningPortMasterServer;
	private static String chunkServerName;
	Scanner sc;
	
	Socket socket;
	DataOutputStream dout;
	DataInputStream din;
	
	/*
	 * Constructor of Chunkserver
	 */
	public Chunkserver(){
		chunkServerName = "Chunkserver";
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
		displayCross();
		
		// input master server's listening port
		do {
			listeningPortMasterServer = inputListeningPortMasterServer();
		} while (listeningPortMasterServer == 0);
		System.out.println( "Master Server listens for requests"			// remove
				+ " on port: " + listeningPortMasterServer);				
		
		// register with master server
		System.out.println("Registering with master server..");
		registerMasterServer();
		
		displayLines();
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
	 * This method is used to take port number that the Chunkserver
	 * listens for connections
	 */
	public int inputListeningPort(){
		sc = new Scanner(System.in);
		int port = 0;
		
		System.out.println("Enter listening port for " + chunkServerName);
		try {
			port = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected for listening"
					+ " port (>1024 && 64k<)");
		}
		return port;
	}
	
	/*
	 * This method is used to take listening port of master server
	 * that the master server uses to listen for incoming requests
	 */
	public int inputListeningPortMasterServer(){
		sc = new Scanner(System.in);
		int port = 0;
		
		System.out.println("Enter listening port that the Master Server"
				+ " is using to listen for incoming requests");
		try {
			port = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected for listening"
					+ " port master server(>1024 && 64k<)");
		}
		return port;
	}
	
	/*
	 * This method is used to register chunkserver.
	 * Chunkserver sends a request to master server. The master server
	 * register's the respective chunkserver upon reception of request
	 * as either chunkserver or backup chunkserver.
	 */
	public void registerMasterServer(){
		String replyFromMasterServer;
		
		try {
			socket = new Socket("localhost", listeningPortMasterServer);
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			
			// send registration request
			dout.writeUTF("Registeration request from " + listeningPort
					+ " " + chunkServerName);
			// receive response from master server
			System.out.println(din.readUTF());
			
		} catch (IOException e) {
			System.err.println("Error encountered while creating a "
					+ "socket!!! " + e);
		} finally {
			try {
				dout.close();
				din.close();
				socket.close();
			} catch (IOException e) {
				System.err.println("Error while closing connections"
						+ " (socket,etc) " + e);
			}
			
		}
	}
	
// ===========================================================================>>>>>>>>
	
	/*
	 * This method is used to get listeningPort
	 */
	public int getListeningPort(){
		return listeningPort;
	}
	
	/*
	 * This method is used to get listeningPort of master server
	 */																			
																			// check if this method is needed 
	public int getListeningPortMasterServer(){
		return listeningPortMasterServer;
	}
	
	/*
	 * This method is used to get chunkserver name
	 */
	public String getChunkserverName(){
		return chunkServerName;
	}
	
// ===========================================================================>>>>>>>>

	/*
	 * This method is used to display lines on console 
	 * for making the screen elegant 
	 */
	public void displayLines(){
		System.out.println("=================================>>");
	}
	
	/*
	 * This method is used to display pluses on console 
	 * for making the screen elegant 
	 */
	public void displayCross(){
		System.out.println("X==X==X==X==X==X==X==X==X==X==X==X=");
	}
	
}
