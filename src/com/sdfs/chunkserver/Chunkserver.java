package com.sdfs.chunkserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Scanner;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

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
		// make directory for this chunkserver
		new File("./" + chunkServerName).mkdir();
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
			dout.writeUTF("Chunkserver Registration request from " + listeningPort
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
					+ "serverSocket!!! (listenRequests)" + e);
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
						+ "client connection!!! (listenRequests)" + e);
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
	boolean stop;
	
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
			
			while(stop == false){
				receivedRequest = din.readUTF();
				
				//System.out.println(receivedRequest); 							// remove for testing
				
				// if the received request is a heartbeat message request
				if(receivedRequest.contains("Heartbeat message"))
					processHeartbeatMessage();
				else if(receivedRequest.contains("Create file request"))
					processCreateFileRequest(receivedRequest);
				else if(receivedRequest.contains("Read request"))
					processReadRequest(receivedRequest);
				
			}
			
			} catch (IOException e) {
			System.err.println("Error encountered while creating "
					+ "data i/p & o/p streams!!! ProcessRequest"
					+ " (Run)" + e);
		}
	}
	
	/*
	 * This method is used to process a heartbeat message
	 */
	public void processHeartbeatMessage(){
		try {
			dout.writeUTF("alive " + chunkServer.getChunkserverName());
			//System.out.println("Response to hearbeat message sent.."); 		// remove for testing
			
		} catch (IOException e) {
			System.err.println("Error encountered while "
					+ "writing through dout!!! " + e);
		}
	}
	
	/*
	 * This method is used to process Create File Request
	 */
	public void processCreateFileRequest(String request){
		String fileName = "", fileContents = "";
		String[] array;
		
		array = request.split(",");
		fileName = array[1];
		System.out.println("Create File request received. "
				+ "Filename: " + fileName);
		try {
			dout.writeUTF("Send file contents");
			fileContents = din.readUTF();
			System.out.println("Contents received"); 						// remove for testing
			
			// Encrypt and store file on the chunkserver
			storeFile(fileName, fileContents);
			
			/**
			 * Send file stored successfully message to client & then
			 * close the socket
			 */
			closeSocket();
			stop = true;
		} catch (IOException e) {
			System.err.println("Error encountered while writing"
					+ " through dout!!! (processCreateFileRequest)"
					+ e);
		}
	}
	
	/*
	 * This method is used create a file. This file is encrypted
	 * and stored on the chunkserver
	 */
	public void storeFile(String fileName, String fileContents){
		KeyGenerator keyGen;
		SecretKey secKey;
		Cipher cipher;
		String encryptedTextString;
		FileWriter fw = null;
		BufferedWriter bw = null;
		
		try {
			keyGen = KeyGenerator.getInstance("AES");
			keyGen.init(128);
			secKey = keyGen.generateKey();
			cipher = Cipher.getInstance("AES");
			
			// encrypt the file contents
			encryptedTextString = encrypt(fileContents, secKey, cipher);
			//plainText = fileContents.getBytes("UTF-8");
//			cipher.init(Cipher.ENCRYPT_MODE, secKey);
	//		encryptedText = cipher.doFinal(plainText);
		//	encryptedTextString = new String(encryptedText);
			
			// write the encrypted file contents
			fw = new FileWriter("./" + chunkServer.getChunkserverName()
				+ "/" + fileName + ".txt");
			bw = new BufferedWriter(fw);
			bw.write(encryptedTextString);
			System.out.println("File: " + fileName + " created"
					+ " successfully");
			
			// send the file name & secret key to master server
			connectMasterServer(fileName, secKey);
			
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Error - while creating keyGen!!! "
					+ "(storeFile) " + e);
		} catch (NoSuchPaddingException e) {
			System.err.println("Error - while creating cipher!!! "
					+ "(storeFile) " + e);
		} catch (UnsupportedEncodingException e) {
			System.err.println("Error - while getting bytes!!! "
					+ "(storeFile) " + e);
		} catch (IOException e) {
			System.err.println("Error - while creating file writer"
					+ " !!! (storeFile) " + e);
		} finally {
			try {
				bw.close();
				fw.close();
			} catch (IOException e) {
				System.err.println("Error - while closing bw/fw"
						+ " !!! (storeFile) " + e);
			}
		}
		
	}
	
	/*
	 * This method is used to encrypt a given plain text string
	 */
	public String encrypt(String plainText, SecretKey secKey,
			Cipher cipher){
		byte[] plainTextByte, encryptedByte;
		String encryptedText = "";
		Base64.Encoder encoder;
		
		plainTextByte = plainText.getBytes();
		try {
			cipher.init(Cipher.ENCRYPT_MODE, secKey);
			encryptedByte = cipher.doFinal(plainTextByte);
			encoder = Base64.getEncoder();
			encryptedText = encoder.encodeToString(encryptedByte);
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return encryptedText;
		
	}
	
	/*
	 * This method is used to send secret key (that was used for
	 * encryption) and file name (respective file that was encrypted)
	 * to the master server 
	 */
	public void connectMasterServer(String fileName, SecretKey
			secKey){
		Socket socket;
		DataOutputStream dout = null;
		String encodedKey;
		
		try {
			// establish connection with the master server
			socket = new Socket("localhost", 
					chunkServer.getListeningPortMasterServer());
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			encodedKey = Base64.getEncoder().encodeToString
					(secKey.getEncoded());
			
			// send secret key & filename
			dout.writeUTF("Secret Key," + fileName + "," 
					+ encodedKey + "," + 
					chunkServer.getChunkserverName());
		} catch (UnknownHostException e) {
			System.err.println("Host unknown!!! "
					+ "(connectedMasterServer) " + e);
		} catch (IOException e) {
			System.err.println("Error - while creating a socket"
					+ " !!! (connectedMasterServer) " + e);
		}
	}
	
	/*
	 * This method is used to process read request
	 */
	public void processReadRequest(String request){
		String[] array;
		String fileName = "", encodedKey = "", fileContents = "", decryptText;
		Socket socket;
		DataInputStream din = null;
		DataOutputStream dout = null;
		byte[] decodedKey;
		SecretKey secKey;
		Cipher cipher;
		FileReader fr = null;
		BufferedReader br = null;
		
		array = request.split(",");
		fileName = array[1];
		System.out.println("Request to read file: " + fileName
				+ ".txt received");
		
		// ask the master server for decryption key
		try {
			socket = new Socket("localhost", 
					chunkServer.getListeningPortMasterServer());
//			System.out.println("Connection established with the master server"); 		// remove
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			dout.writeUTF("Decryption key," + fileName);
//			System.out.println("Decryption key request sent to master"); 				// remove
			encodedKey = din.readUTF();
//			System.out.println("decryption key received"); 								// remove
			din.close();
			dout.close();
			socket.close();
			
			decodedKey = Base64.getDecoder().decode(encodedKey);
			secKey = new SecretKeySpec(decodedKey, 0,
					decodedKey.length, "AES");
			
			// open and read file contents
			fr = new FileReader("./" + chunkServer.getChunkserverName() + "/" + fileName + ".txt");
			br = new BufferedReader(fr);
			String line;
			while((line = br.readLine()) != null){
				fileContents += line;
			}
//			System.out.println("Read file contents :" + fileContents + ":"); remove
			
			cipher = Cipher.getInstance("AES");
			// get decrypted text
			decryptText = decrypt(fileContents, secKey, cipher);
//			cipher.init(Cipher.DECRYPT_MODE, secKey);
			
//			decryptedText = cipher.doFinal(encryptedText);
	//		decryptText = new String(decryptedText);
//			System.out.println("Contents read from the file are:"); 			// remove testing
//			System.out.println(decryptText);
			this.dout.writeUTF(decryptText);
//			System.out.println("contents sent");			remove
			stop = true;
			closeSocket();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	/*
	 * This method is used to decrypt an encrypted string
	 */
	public String decrypt(String encryptedText, SecretKey secKey,
			Cipher cipher){
		Base64.Decoder decode;
		byte[] encryptedTextByte, decryptedByte;
		String decryptedText = "";
		
		decode = Base64.getDecoder();
		encryptedTextByte = decode.decode(encryptedText);
		try {
			cipher.init(Cipher.DECRYPT_MODE, secKey);
			decryptedByte = cipher.doFinal(encryptedTextByte);
			decryptedText = new String(decryptedByte);
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return decryptedText;
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
