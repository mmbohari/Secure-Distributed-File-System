package com.sdfs.masterServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
	HashMap<String, Integer> chunkserverMap;
	HashMap<String, String> usernamePassMap;

	/*
	 * Constructor of MasterServer
	 */
	public MasterServer() {
		keepListening = true;
		activeChunkserverArray = new String[3];
		backupChunkserverArray = new String[3];
		chunkserverMap = new HashMap<>();
		usernamePassMap = new HashMap<>();
	}

	/*
	 * This method initializes Master Server i.e. takes listening port
	 */
	public void initialise() {
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
		System.out.println("Registration request received from" + " " + array[array.length - 1]);

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
	public void assignChunkserver(DataOutputStream dout, String chunkserverName, int chunkserverListeningPort) {
		activeChunkserver++;
		activeChunkserverArray[activeChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, chunkserverListeningPort);

		try {
			dout.writeUTF("Your request has been received. You are" + " registered as Chunkserver.");

			// create a thread that will send heartbeat messages to
			// this chunkserver
			Thread thread = new Thread(new Heartbeat(this, chunkserverName));
			thread.start();
		} catch (IOException e) {
			System.err.println("Error encountered while writing" + " with data o/p!!! (assignChunkserver)" + e);
		}
	}

	/*
	 * This method is used to assign role of backup Chunkserver
	 */
	public void assignBackupChunkserver(DataOutputStream dout, String chunkserverName, int chunkserverListeningPort) {
		backupChunkserver++;
		backupChunkserverArray[backupChunkserver - 1] = chunkserverName;
		chunkserverMap.put(chunkserverName, chunkserverListeningPort);

		try {
			dout.writeUTF("Your request has been received. You are" + " registered as Backup Chunkserver.");
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

		System.out.println("Client registration request received."); // Use for
																		// Audits
		try {
			dout.writeUTF("Enter a username and password");
			array = (din.readUTF()).split(",");

			passwordHash = BCrypt.hashpw(array[1], BCrypt.gensalt());
			// System.out.println("Hashed pass: " + passwordHash); // remove
			usernamePassMap.put(array[0], passwordHash);
			dout.writeUTF("Client with username: " + array[0] + " has" + " been registered successfully.");
			requestOutcome = "Success";

			System.out.println("Client Registration Request " + " from: " + array[0] + " Outcome: " + requestOutcome); // Use
																														// for
																														// Audits
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
		System.out.println("Login request received from client with" + " username: " + array[1]); // Use
																									// for
																									// Audits
		// check if username exists
		if (usernamePassMap.containsKey(array[1])) {
			if (BCrypt.checkpw(array[2], usernamePassMap.get(array[1]))) {
				try {
					dout.writeUTF("Login successful");
					requestOutcome = "Success";
				} catch (IOException e) {
					System.err.println("Error encountered while using" + " dout!!! " + e);
				}
			} else {
				try {
					dout.writeUTF("Invalid username or password!!!");
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
