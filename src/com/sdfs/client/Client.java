package com.sdfs.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

/**
 * 
 * This class represents client.
 * 
 * @author mmbohari
 *
 */
public class Client {

	Scanner sc;
	boolean stop, loggedIn, socketClosed;
	static int masterServerListeningPort;
	static String username;
	static String password;
	
	/*
	 * This method is used to initialize the client
	 */
	public void initialise(){
		int operation;
		inputMasterServerListeningPort();
		
		while(stop == false){
			displayLines();
			// take input
			operation = input();
			// process input
			processOperation(operation);
			displayLines();
		}
		
	}
	
	/*
	 * This method is used to take input from client
	 */
	public int input(){
		int operation = 0;
		sc = new Scanner(System.in);

		System.out.println("Welcome client, please enter the number"
				+ " attached to respective options that you want to"
				+ " perform");
		
		do {
			try {
				displayOperations();
				operation = sc.nextInt();
			} catch (Exception e) {
				System.err.println("Integer value expected!!!");
			}
		} while (operation == 0);
		
		return operation;
	}
	
	/*
	 * This method is used to process operation selected by the user
	 */
	public void processOperation(int operation){
		boolean stop = false;
		sc = new Scanner(System.in);
		int option = 0;
		
		displayCross();
		
		// if operation is Register, then perform registration 
		// request
		if(operation == 1){
			// send a registration request to master server
			sendRegistrationRequest();
		} else {
			// send login request
			sendLoginRequest();
			
			// check if login was successful
			if(loggedIn == false)
				stop = true;
			while(stop == false){
				displayAdvanceOperations();
				
				do{
					try {
						option = sc.nextInt();
						
						if(option == 1){
							sendCreateRequest();
						} else if(option == 2){
							/**
							 * Code for appending data to a file
							 */
						} else if(option == 3){
							/**
							 * Code for reading a file
							 */
						} else if(option == 4){
							/**
							 * Code for deleting a file
							 */
						} else
							stop = true;
					} catch (Exception e) {
						System.err.println("Integer value expected!!!");
					}
				} while(option == 0);
			}
		}

	}
	
	/*
	 * This method is used to get listening port that the master
	 * server is using to accept connections
	 */
	public void inputMasterServerListeningPort(){
		sc = new Scanner(System.in);
		System.out.println("Enter Master Server listening port....");
		
		try {
			masterServerListeningPort = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Integer value expected!!!");
		}
	}
	
	public void sendRegistrationRequest(){
		Socket socket = null;
		
		socket = establishConnection(socket, masterServerListeningPort);
		
		try {
			DataInputStream din = new DataInputStream(socket.
					getInputStream());
			DataOutputStream dout = new DataOutputStream(socket.
					getOutputStream());
			
			dout.writeUTF("Client Registration request");					
			din.readUTF();
			// get username & password
			String user = inputUsername();
			String pass = inputPassword();
			// send username & password to master server
			dout.writeUTF(user + "," + pass);
			System.out.println(din.readUTF());
			
			closeConnection(socket, din, dout);
		} catch (IOException e) {
			System.err.println("Error encountered while creating"
					+ " din/dout!!! " + e);
		}
		
	}
	
	/*
	 * This method is used to input username
	 */
	public String inputUsername(){
		String user;
		sc = new Scanner(System.in);
		
		System.out.println("Enter a username");
		user = sc.nextLine();
		
		return user;
	}
	
	/*
	 * This method is used to input password
	 */
	public String inputPassword(){
		String pass;
		sc = new Scanner(System.in);

		System.out.println("Enter a password");
		pass = sc.nextLine();
		
		return pass;
	}
	
	/*
	 * This method is used to send login credentials to the master
	 * server
	 */
	public void sendLoginRequest(){
		Socket socket = null;
		String user, pass, replyFromMS;
		DataInputStream din = null;
		DataOutputStream dout = null;
		
		socket = establishConnection(socket, masterServerListeningPort);
		try {
			din = new DataInputStream(socket.
					getInputStream());
			dout = new DataOutputStream(socket.
					getOutputStream());
			
			// get username & password
			user = inputUsername();
			pass = inputPassword();
			// send login credentails to the master server
			dout.writeUTF("ClientLoginRequest," + user +"," + pass);
			replyFromMS = din.readUTF();
			System.out.println(replyFromMS);
			if(replyFromMS.contains("Login successful")){
				loggedIn = true;
				username = user;
			}
				
			
			
		} catch (IOException e) {
			System.err.println("Error encountered while creating"
					+ " din/dout!!! " + e);
		} finally{
			try {
				din.close();
				dout.close();
			} catch (IOException e) {
				System.err.println("Error encountered while closing"
						+ " din/dout (fromSendLoginRequest)");
			}
		}
		
		displayLines();
	}
	
	/*
	 * This method is used to send a request to create a file in SDFS.
	 * Upon reception of this request, the master server provides
	 * chunkserver port where the client has to forward name and contents
	 * of the file that is to be created.
	 */
	public void sendCreateRequest(){
		Socket socket = null;
		DataInputStream din = null;
		DataOutputStream dout = null;
		String fileName = "", fileContents;
		String replyFromMS;
		String array[];
		
		displayCross();
		socket = establishConnection(socket, masterServerListeningPort);
		sc = new Scanner(System.in);

		try {
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			
			// send create file request to master server
			dout.writeUTF("Create file request," + username);
			replyFromMS = din.readUTF();
			array = replyFromMS.split(",");
			System.out.println("Send create request to "			// Remove for testing
					+ array[0] + " at port: " + array[1]);
			closeConnection(socket, din, dout);
			
			// establish connection with the respective chunkserver
			socket = establishConnection(socket, 
					Integer.parseInt(array[1]));
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());

			// Send create file request to chunkserver along with
			// filename
			System.out.println("Enter filename:");
			fileName = sc.nextLine();
			dout.writeUTF("Create file request," + fileName);
			displayCross();
			// send file contents to chunkserver
			System.out.println("Enter file contents");
			fileContents = sc.nextLine();
			dout.writeUTF(fileContents);
			displayCross();
			System.out.println(din.readUTF()); 								// Remove for testing

		} catch (IOException e) {
			System.err.println("Error encountered while creating"
					+ " din/dout!!! (sendCreateRequest) " + e);
		}
			closeConnection(socket, din, dout);
	}
	
// ===========================================================================>>>>>>>>
	
	/*
	 * This method is used to display possible operations
	 * that a user can perform
	 */
	public void displayOperations(){
		System.out.println("1. Register");
		System.out.println("2. Login");
	}
	
	/*
	 * This method is used to display advance operations
	 * that a user can perform
	 */
	public void displayAdvanceOperations(){
		System.out.println("1. Create a file");
		System.out.println("2. Append data to an existing file");
		System.out.println("3. Read a file");
		System.out.println("4. Delete a file");
		System.out.println("0<>5. Logout");
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
	
	/*
	 * This method is used to establish connection with master
	 * server
	 */
	public Socket establishConnection(Socket socket, int port){
		try {
			socket = new Socket("localhost", port);
		} catch (IOException e) {
			System.err.println("Error encountered while creating a "
					+ "socket!!! " + e);
		}
		return socket;
	}
	
	/*
	 * This method is used to close a connection/ socket
	 */
	public void closeConnection(Socket socket, DataInputStream din
			, DataOutputStream dout){
		try {
			din.close();
			dout.close();
			socket.close();
		} catch (IOException e) {
			System.err.println("Error encountered while closing"
					+ " connection!!! (closeConnection) " + e);
		}
	}
	
}
