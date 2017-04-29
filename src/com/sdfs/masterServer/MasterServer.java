package com.sdfs.masterServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
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

	private static int listeningPort;
	private static int activeChunkserver;
	private static int backupChunkserver;
	private static String[] activeChunkserverArray;
	private static String[] backupChunkserverArray;
	Scanner sc;
	boolean keepListening;
	
	ServerSocket serverSocket;
	Socket clientSocket;												// can make this local						
	HashMap<String, Integer> chunkserverMap;
	
	/*
	 * Constructor of MasterServer
	 */
	public MasterServer(){
		keepListening = true;
		activeChunkserverArray = new String[3];
		backupChunkserverArray = new String[3];
		chunkserverMap = new HashMap<>();
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
		// listen for incoming connections
		listenRequests();
		
		displayLines();
		displayLines();
	}
	
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
	 * This method is used to listen for incoming requests.
	 * Chunkserver or client can send requests 
	 */
	public void listenRequests(){
		try {
			serverSocket = new ServerSocket(listeningPort);
		} catch (IOException e) {
			System.err.println("Error encountered while opening "
					+ "serverSocket!!! " + e);
		}
		
		while(keepListening){
			displayLines();
			System.out.println("Master Server listening for connections"
					+ " on port: " + listeningPort);					// remove displaying listeningPort
			
			try {
				clientSocket = serverSocket.accept();
				processRequest(clientSocket);							
				
			} catch (IOException e) {
				System.err.println("Error encountered while accepting "
						+ "client connection!!! " + e);
			} finally {
				try {
					clientSocket.close();
				} catch (IOException e) {
					System.err.println("Error while closing socket "
							+ "connection!!! " + e);
				}
			}
			
			// for testing purposes
			displayCross();													// remove
			displayActiveChunkserver();										// remove
			displayBackupChunkserver();										// remove
		}
	}
	
	/*
	 * This method is used to process requests received by the master server
	 */
	public void processRequest(Socket socket){
		String receivedRequest;
		DataInputStream din = null;
		DataOutputStream dout = null;
		
		try {
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			receivedRequest = din.readUTF();
			
			if(receivedRequest.contains("Registeration request"))
				registrationRequest(receivedRequest, dout);

			} catch (IOException e) {
			System.err.println("Error encountered while creating "
					+ "data i/p & o/p streams!!! " + e);
		} finally {
			try {
				din.close();
				dout.close();
			} catch (IOException e) {
				System.err.println("Error while data i/p & o/p streams"
						+ "!!! " + e);
			}
			
		}
		
	}
	
	/*
	 * This method is used to process registration request
	 * received by the master server.
	 */
	public void registrationRequest(String request, 
			DataOutputStream dout){
		int chunkserverListeningPort = 0;
		
		String[] array = request.split(" ");
		System.out.println("Registration request received from"
				+ " " + array[array.length - 1]);
		
		try{
			chunkserverListeningPort = Integer.parseInt(
					array[array.length - 2]);
		} catch(Exception e){
			System.err.println("Error encountered while parsing"
					+ " chunkserver listening port!!! " + e);
		}
		
		
		if(activeChunkserver <3)						// if active chunkserver no. is increased, then increase activeChunkserverArray
			assignChunkserver(dout, array[array.length - 1], 
					chunkserverListeningPort);
		else if(backupChunkserver < 3)
			assignBackupChunkserver(dout, array[array.length - 1], 
					chunkserverListeningPort);
		else{
			try {
				dout.writeUTF("No role assigned as full capacity "
						+ "of chunkserver and backup chunkserver"
						+ " has been reached");
			} catch (IOException e) {
				System.err.println("Error encountered while writing "
						+ "in dout!!! " + e);
			}
		}
			
		/**
		 * write a method to assign as backup chunkserver
		 * 
		 */
		
	}
	
	/*
	 * This method is used to assign role of chunkserver
	 */
	public void assignChunkserver(DataOutputStream dout,
			 String chunkserverName, int chunkserverListeningPort){
		activeChunkserver++;
		activeChunkserverArray[activeChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, 
				chunkserverListeningPort);
		
		try {
			dout.writeUTF("Your request has been received. You are"
					+ " registered as Chunkserver.");
		} catch (IOException e) {
			System.err.println("Error encountered while writing"
					+ " with data o/p!!! " + e);
		}
	}
	
	/*
	 * This method is used to assign role of backup Chunkserver
	 */
	public void assignBackupChunkserver(DataOutputStream dout,
			 String chunkserverName, int chunkserverListeningPort){
		backupChunkserver++;
		backupChunkserverArray[backupChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, 
				chunkserverListeningPort);
		
		try {
			dout.writeUTF("Your request has been received. You are"
					+ " registered as Backup Chunkserver.");
		} catch (IOException e) {
			System.err.println("Error encountered while writing"
					+ " with data o/p!!! " + e);
		}
	}
	

// ===========================================================================>>>>>>>>	
	
	/*
	 * This method is used to see list of active chunkservers & their
	 * listening ports
	 */
	public void displayActiveChunkserver(){
		// display information of activeChunkserverArray
		for(int i=0; i < activeChunkserver; i++){
			System.out.println(activeChunkserverArray[i] + " "
					+ chunkserverMap.get(activeChunkserverArray[i]));
		}
		
	}
	
	/*
	 * This method is used to see list of backup chunkservers & their
	 * listening ports
	 */
	public void displayBackupChunkserver(){
		if(backupChunkserver != 0)
		
		// display information of backupChunkserverArray
		for(int i=0; i < backupChunkserver; i++){
			System.out.println(backupChunkserverArray[i] + " "
					+ chunkserverMap.get(backupChunkserverArray[i]));
		}
		
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
	
	/*
	 * This method is used to display pluses on console 
	 * for making the screen elegant 
	 */
	public void displayCross(){
		System.out.println("X==X==X==X==X==X==X==X==X==X==X==X=");
	}
	
}
