package com.sdfs.chunkserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
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
	private static boolean keepListening;
	Scanner sc;
	
	Socket socket;
	
	ServerSocket serverSocket;
	Socket clientSocket;
	
	/*
	 * Constructor of Chunkserver
	 */
	public Chunkserver(){
		chunkServerName = "Chunkserver";
		keepListening = true;
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
		
		// listen for requests
		listenRequests();
		
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
		DataInputStream din = null;
		DataOutputStream dout = null;
		
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
	
	/*
	 * This method is used to listen for incoming requests.
	 * Master server or client can send requests 
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
			System.out.println(chunkServerName +  " listening for connections"
					+ " on port: " + listeningPort);					// remove displaying listeningPort
			
			try {
				clientSocket = serverSocket.accept();
				// establish a new thread to process the received request
				Thread thread = new Thread(new 
						ProcessRequest(clientSocket, this));
				thread.start();
				
			} catch (IOException e) {
				System.err.println("Error encountered while accepting "
						+ "client connection!!! " + e);
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

/**
 * This class is used to process request received 
 * by chunkserver. Requests could be either sent by master server
 * or clients
 */
class ProcessRequest implements Runnable{

	Socket socket;
	DataInputStream din;
	DataOutputStream dout;
	Chunkserver chunkServer;
	
	/*
	 * This is constructor method.
	 */
	public ProcessRequest(Socket socket, Chunkserver chunkObject){
		this.socket = socket;
		chunkServer = chunkObject;
	}
	
	public void run() {
		String receivedRequest;
		
		try {
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			
			while(true){
				receivedRequest = din.readUTF();
				
				System.out.println(receivedRequest); 							// remove for testing
				
				// if the received request is a heartbeat message request
				if(receivedRequest.contains("Heartbeat message"))
					processHeartbeatMessage();
			}
			
			} catch (IOException e) {
			System.err.println("Error encountered while creating "
					+ "data i/p & o/p streams!!! " + e);
		}
	}
	
	/*
	 * This method is used to process a heartbeat message
	 */
	public void processHeartbeatMessage(){
		try {
			dout.writeUTF("alive " + chunkServer.getChunkserverName());
			System.out.println("Response to hearbeat message sent.."); 		// remove for testing
			
		} catch (IOException e) {
			System.err.println("Error encountered while "
					+ "writing through dout!!! " + e);
		}
	}
	
	/*
	 * This method is used to close socket along with
	 * din and dout
	 */
	public void closeSocket(){
		try {
			din.close();
			dout.close();
			socket.close();
		} catch (IOException e) {
			System.err.println("Error while closing socket or "
					+ "din/dout !!! " + e);
		}
	}
	
}
