package com.sdfs.masterServer;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;


/**
 * 
 * This class represents Master Server. Master Server listens for requests on
 * listeningPort.
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
	Socket clientSocket; // can make this local
	HashMap<String, Integer> chunkserverMap;		// Chunkserver Name & their listening port
	HashMap<String, String> usernamePassMap;		// Username & hashKey of their password
	HashMap<String, String> fileKeyMap;				// Filename & secretKey (String format)
	HashMap<String, String> filePrimaryMap;			// Filename & chunkserver where it is present

	/*
	 * Constructor of MasterServer
	 */
	public MasterServer() {
		keepListening = true;
		activeChunkserverArray = new String[3];
		backupChunkserverArray = new String[3];
		chunkserverMap = new HashMap<>();
		usernamePassMap = new HashMap<>();
		fileKeyMap = new HashMap<>();
		filePrimaryMap = new HashMap<>();
	}

	/*
	 * This method initializes Master Server i.e. takes listening port
	 */
	public void initialise() {
		displayLines();
		System.out.println("Initialising master");
		Thread thread = new Thread(new 
				Audit("=========================================>>"
						+ "\n" + "SDFS: The master server started."));
		thread.start();

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
	 * This method is used to take port number that the Master Server will
	 * listen connection on
	 */
	public int inputListeningPort() {
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
	 * This method is used to listen for incoming requests. Chunkserver or
	 * client can send requests
	 */
	public void listenRequests() {
		try {
			serverSocket = new ServerSocket(listeningPort);
		} catch (IOException e) {
			System.err.println("Error encountered while opening "
					+ "serverSocket!!! " + e);
		}

		while (keepListening) {
			displayLines();
			System.out.println("Master Server listening for "
					+ "connections" + " on port: " + listeningPort); // remove
																											// displaying
																											// listeningPort

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
			displayCross(); // remove
			// displayActiveChunkserver(); // remove
			// displayBackupChunkserver(); // remove
		}
	}

	/*
	 * This method is used to process requests received by the master server
	 */
	public void processRequest(Socket socket) {
		String receivedRequest;
		DataInputStream din = null;
		DataOutputStream dout = null;

		try {
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
			receivedRequest = din.readUTF();

			if (receivedRequest.contains("Chunkserver Registration request"))
				chunkserverRegistrationRequest(receivedRequest, dout);
			else if (receivedRequest.contains("Client Registration request"))
				clientRegistrationRequest(din, dout);
			else if (receivedRequest.contains("ClientLoginRequest"))
				clientLoginRequest(din, dout, receivedRequest);
			else if (receivedRequest.contains("Create file request"))
				createFileRequest(dout, receivedRequest);
			else if (receivedRequest.contains("Secret Key"))
				storeSecretKey(receivedRequest);
			else if (receivedRequest.contains("Read file request"))
				readFileRequest(receivedRequest,dout);
			else if(receivedRequest.contains("Decryption key"))
				giveDecryptionKey(receivedRequest, dout);
			else if(receivedRequest.contains("Delete request"))
				deleteRequest(receivedRequest, dout);

		} catch (IOException e) {
			System.err.println("Error encountered while creating " + "data i/p & o/p streams!!! (processRequest) " + e);
		} finally {
			try {
				din.close();
				dout.close();
			} catch (IOException e) {
				System.err.println("Error while data i/p & o/p streams" + "!!! " + e);
			}

		}

	}

	/*
	 * This method is used to process registration request received by the
	 * master server.
	 */
	public void chunkserverRegistrationRequest(String request, DataOutputStream dout) {
		int chunkserverListeningPort = 0;
		
		String[] array = request.split(" ");
		// write log
		new Thread(new Audit(array[array.length - 1] + " Registration requested received")).start();
		System.out.println("Registration request received from" + " " + array[array.length - 1] + ".");

		try {
			chunkserverListeningPort = Integer.parseInt(array[array.length - 2]);
		} catch (Exception e) {
			System.err.println("Error encountered while parsing" + " chunkserver listening port!!! "
					+ "(chunkserverRegistrationRequest)" + e);
		}

		if (activeChunkserver < 3) // if active chunkserver no. is increased,
									// then increase activeChunkserverArray
			assignChunkserver(dout, array[array.length - 1], chunkserverListeningPort);
		else if (backupChunkserver < 3)
			assignBackupChunkserver(dout, array[array.length - 1], chunkserverListeningPort);
		else {
			try {
				dout.writeUTF("No role assigned as full capacity " + "of chunkserver and backup chunkserver"
						+ " has been reached");
			} catch (IOException e) {
				System.err.println(
						"Error encountered while writing " + "in dout!!! (chunkserverRegistrationRequest)" + e);
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
	public void assignChunkserver(DataOutputStream dout, String 
			chunkserverName, int chunkserverListeningPort) {
		activeChunkserver++;
		activeChunkserverArray[activeChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, chunkserverListeningPort);

		try {
			dout.writeUTF("Your request has been received. You are" 
		+ " registered as Chunkserver.");
			// write log
			new Thread(new Audit(chunkserverName + " assigned "
					+ "chunkserver role.")).start();

			// create a thread that will send heartbeat messages to
			// this chunkserver
			Thread thread = new Thread(new Heartbeat(this, 
					chunkserverName));
			thread.start();
		} catch (IOException e) {
			System.err.println("Error encountered while writing" + " "
					+ "with data o/p!!! (assignChunkserver)" + e);
		}
	}

	/*
	 * This method is used to assign role of backup Chunkserver
	 */
	public void assignBackupChunkserver(DataOutputStream dout, 
			String chunkserverName, int chunkserverListeningPort) {
		backupChunkserver++;
		backupChunkserverArray[backupChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, chunkserverListeningPort);

		try {
			dout.writeUTF("Your request has been received. You are" 
					+ " registered as Backup Chunkserver.");
			// write log
			new Thread(new Audit(chunkserverName + " assigned backup "
					+ "chunkserver role.")).start();

		} catch (IOException e) {
			System.err.println("Error encountered while writing" + " with data o/p!!! " + e);
		}
	}

	/*
	 * This method is used to process client registration requests
	 */
	public void clientRegistrationRequest(DataInputStream din, DataOutputStream dout) {
		String[] array;
		String passwordHash, requestOutcome = "Failed";

		System.out.println("Client registration request received."); 
		// writing log
		new Thread(new Audit("Client registration request received.")).start();
		
		try {
			dout.writeUTF("Enter a username and password");
			array = (din.readUTF()).split(",");

			passwordHash = BCrypt.hashpw(array[1], BCrypt.gensalt());
			// System.out.println("Hashed pass: " + passwordHash); // remove
			usernamePassMap.put(array[0], passwordHash);
			dout.writeUTF("Client with username: " + array[0] + " has" +
					" been registered successfully.");
			// writing log
			new Thread(new Audit("Client with username :" + array[0]
					+ ": has" + " been registered successfully.")).start();
			requestOutcome = "Success";

			System.out.println("Client Registration Request " + " from :" + array[0] + ": Outcome: " + requestOutcome); 
		} catch (IOException e) {
			System.err.println("Error encountered while using dout!!! " + "clientRegistrationRequest " + e);
		}
	}

	/*
	 * This method is used to process client login request. If the username and
	 * passwords match, then a successful login message is sent, else login fail
	 * message is sent
	 */
	public void clientLoginRequest(DataInputStream din, DataOutputStream dout, String loginRequest) {
		String[] array;
		String requestOutcome = "Failed";

		array = loginRequest.split(",");
		System.out.println("Login request received from client with" + " username: " + array[1]); 
		// writing log
		new Thread(new Audit("Login request received from client with" + " username :" + array[1]
				+ ":")).start();
		
		// check if username exists
		if (usernamePassMap.containsKey(array[1])) {
			if (BCrypt.checkpw(array[2], usernamePassMap.get(array[1]))) {
				try {
					dout.writeUTF("Login successful");
					new Thread(new Audit("Client :" + array[1] + ": login successfully")).start();
					requestOutcome = "Success";
				} catch (IOException e) {
					System.err.println("Error encountered while using" + " dout!!! " + e);
				}
			} else {
				try {
					dout.writeUTF("Invalid username or password!!!");
					new Thread(new Audit("Client: " + array[1] + " loggen in unsuccessful")).start();
				} catch (IOException e) {
					System.err.println("Error encountered while using" + " dout!!! " + e);
				}
			}
		} else {
			try {
				dout.writeUTF("Invalid username or password!!!");
			} catch (IOException e) {
				System.err.println("Error encountered while using" + " dout!!! " + e);
			}
		}
		System.out.println("Login Request Outcome: " + requestOutcome); // Use
																		// for
																		// Audits
	}

	/*
	 * This method is used process create file request. The master server
	 * forwards the chunkserver name & its listening port to the client.
	 */
	public void createFileRequest(DataOutputStream dout, String request) {
		int chunkserverNo = -1;
		int csListeningPort = -1;
		String csName;

		// Display the request received // Use for Audits
		String[] array = request.split(",");
		System.out.println("Create File Request received from: " + array[1]);
		// writing log
		new Thread(new Audit("Create File Request from Client: " + array[1])).start();

		chunkserverNo = randomNoGenerator();
		// System.out.println("Random Number: " + chunkserverNo); // remove
		csName = "Chunkserver" + chunkserverNo;
		csListeningPort = chunkserverMap.get(csName);
		try {
			dout.writeUTF(csName + "," + csListeningPort);
		} catch (IOException e) {
			System.err.println("Error encountered while using dout!!!" + " (createFileRequest)");
		} finally {
			try {
				dout.close();
			} catch (IOException e) {
				System.err.println("Error encountered while closing" + " dout!!! (createFileRequest)");
			}
		}
	}
	
	/*
	 * This method is used to store file name & its secret key
	 */
	public void storeSecretKey(String request){
		String array[];
		String filename, encodedKey, chunkserverName;
		
//		System.out.println("Received secret key"); 							// remove Testing
		array = request.split(",");
		filename = array[1];
		encodedKey = array[2];
		chunkserverName = array[3];
		
		// store file name & its secret key
		fileKeyMap.put(filename, encodedKey);
		// writing log
		new Thread(new Audit(filename + ".txt Created")).start();
		
//		System.out.println("Filename :" + filename + ": Stored encoded key :" +fileKeyMap.get(filename) + ":"); 			// remove
//		System.out.println("is the filename present in filePrimaryMap: " + fileKeyMap.containsKey(filename));				// remove
		// store file name & chunkserver where it is present
		filePrimaryMap.put(filename, chunkserverName);
//		System.out.println("File name & key stored"); 						// remove Testing
	}
	
	/*
	 * This method is used to process read file request
	 */
	public void readFileRequest(String request, DataOutputStream
			dout){
		String[] array;
		String filename, chunkserverName, username;
		int chunkListeningPort = 0;
		
		array = request.split(",");
		filename = array[1];
		username = array[2];
		chunkserverName = filePrimaryMap.get(filename);
		chunkListeningPort = chunkserverMap.get(chunkserverName);

		new Thread(new Audit("Read Reqeuest for " + filename + ".txt from Client: " + username)).start();
		System.out.println( filename + "is present at" + 			// remove
				chunkserverName);
		// send port number of chunkserver that has the file
		try {
			dout.writeUTF(chunkserverName + "," + chunkListeningPort);
		} catch (IOException e) {
			System.err.println("Error while writing through dout");
		}
		
	}
	
	public void giveDecryptionKey(String request, DataOutputStream dout){
		String[] array;
		String filename = "";
		String encodedKey = "";
		
//		System.out.println("Decrypton key request received");
		array = request.split(",");
		filename = array[1];
//		System.out.println("File name :" + filename + ": ");  							// remove testing
		encodedKey = fileKeyMap.get(filename);
//		System.out.println("Fetched encoded key :" + encodedKey + ":"); 						// remove
		try {
			dout.writeUTF(encodedKey);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * This method is used to process delete file request
	 */
	public void deleteRequest(String request, DataOutputStream dout){
		String filename = "", username = "", chunkserver = "";
		String[] array;
		File file;
		
		array = request.split(",");
		filename = array[1];
		username = array[2];
		System.out.println("Delete request for file :" + filename + ".txt received");
		// write log
		new Thread(new Audit("Delete Request for File: " + filename + ".txt "
				+ "from Client: " + username)).start();
		
		// check if the file is present
		if(fileKeyMap.containsKey(filename)){
			System.out.println("File is present. Progressing to delete operation");
			chunkserver = filePrimaryMap.get(filename);
			
			file = new File("./" + chunkserver + "/" + filename + ".txt");
			if(file.delete()){
    			System.out.println(file.getName() + " is deleted!");
    			// write in log
    			new Thread(new Audit(filename + ".txt Deleted")).start();
    			fileKeyMap.remove(filename);
    			filePrimaryMap.remove(filename);
    			try {
					dout.writeUTF(filename + ".txt deleted successfully");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
			/**
			 * Write program for deleting the respective file
			 */
		} else{
			System.out.println("File is not present");
			// write log
			new Thread(new Audit(filename + " is not present. Unsuccessful delete operation from Client: " + username)).start();
			try {
				dout.writeUTF(filename + ".txt is not present");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				dout.writeUTF(filename + " cannot be deleted");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * This method is used to delete a respective file
	 */
	public void deleteFile(String filename){
		
	}

	// ===========================================================================>>>>>>>>

	/*
	 * This method is used to see list of active chunkservers & their listening
	 * ports
	 */
	public void displayActiveChunkserver() {
		System.out.println("Active Chunkserver list & their listening port's");

		// display information of activeChunkserverArray
		for (int i = 0; i < activeChunkserver; i++) {
			System.out.println(activeChunkserverArray[i] + " " + chunkserverMap.get(activeChunkserverArray[i]));
		}

	}

	/*
	 * This method is used to see list of backup chunkservers & their listening
	 * ports
	 */
	public void displayBackupChunkserver() {
		if (backupChunkserver != 0)
			System.out.println("Backup Chunkserver list & their listening port's");

		// display information of backupChunkserverArray
		for (int i = 0; i < backupChunkserver; i++) {
			System.out.println(backupChunkserverArray[i] + " " + chunkserverMap.get(backupChunkserverArray[i]));
		}
	}

	/*
	 * This method is used to get listeningPort
	 */
	public int getListeningPort() {
		return listeningPort;
	}

	/*
	 * This method is used to display lines on console for making the screen
	 * elegant
	 */
	public void displayLines() {
		System.out.println("=================================>>");
	}

	/*
	 * This method is used to display pluses on console for making the screen
	 * elegant
	 */
	public void displayCross() {
		System.out.println("X==X==X==X==X==X==X==X==X==X==X==X=");
	}

	/*
	 * This method is used to generate a random number between 0 & 2
	 */
	public int randomNoGenerator() {
		int randomNo = -1;
		Random random = new Random();
		randomNo = random.nextInt(activeChunkserver);

		return randomNo + 1;
	}
}

/**
 * This class is used to send heartbeat messages to chunkservers. Failure is
 * detected through this class
 * 
 * @author mmbohari
 *
 */
class Heartbeat implements Runnable {

	MasterServer masterServer;
	Socket socket;
	DataInputStream din;
	DataOutputStream dout;
	int chunkserverListeningPort;
	String chunkserverName;
	boolean stop;

	public Heartbeat(MasterServer masterServer, String chunkserverName) {
		this.masterServer = masterServer;
		this.chunkserverName = chunkserverName;
		chunkserverListeningPort = masterServer.chunkserverMap.get(chunkserverName);
	}

	public void run() {
		String hearbeatReply;

		// establish connection with chunkserver
		establishConnection();

		// keep sending heartbeat messages in a interval of 5 seconds
		while (stop == false) {
			try {
				dout.writeUTF("Heartbeat message from Master Server");
				hearbeatReply = din.readUTF();
				// System.out.println("Heartbeat message received from "
				// + "" + chunkserverName); // remove testing purpose
			} catch (IOException e1) {
				System.err.println(chunkserverName + " is dead!!!");
				stop = true;
				masterServer.displayCross();

				new Thread(new Audit(chunkserverName + " is dead")).start();
				/**
				 * Write code for taking action once it is found that a
				 * chunkserver is dead
				 */
			}

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				System.err.println("Error while thread is trying to " + "sleep!!! " + e);
			}
		}
	}

	/*
	 * This method is used to establish connection with chunkserver
	 */
	public void establishConnection() {
		try {
			socket = new Socket("localhost", chunkserverListeningPort);
			din = new DataInputStream(socket.getInputStream());
			dout = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			System.err.println("Error encountered (From class Heartbear)" + " " + e);
		}
	}

	/*
	 * This method is used to close socket along with din and dout
	 */
	public void closeSocket() {
		try {
			din.close();
			dout.close();
			socket.close();
		} catch (IOException e) {
			System.err.println("Error while closing socket or " + "din/dout !!! " + e);
		}
	}

}

/**
 * This class is used to write an audit log for SDFS.
 * 
 * @author mmbohari
 *
 */
class Audit implements Runnable{

	String log, filepath;
	FileWriter fw = null;
	BufferedWriter bw = null;
	Timestamp timestamp;
	
	public Audit(String log){
		this.log = log;
		filepath = "./AuditLog.txt";
	}
	
	public void run() {
		try {
			fw = new FileWriter(filepath, true);
			bw = new BufferedWriter(fw);
			bw.newLine();
			bw.write(log + "							" + new Timestamp(System.currentTimeMillis()));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
