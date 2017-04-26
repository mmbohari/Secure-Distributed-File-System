package com.sdfs.main;

import java.util.Scanner;

import com.sdfs.chunkserver.Chunkserver;
import com.sdfs.client.Client;
import com.sdfs.masterServer.MasterServer;

/**
 * 
 * This class is used to launch Secure Distributed File System (SDFS).
 * Based upon the input, it launches either master server, chunkserver,
 * client or backup chunkserver.
 * 
 * @author mmbohari
 *
 */

public class Sdfs {
	Scanner sc;
	
	/*
	 * This method displays the available options to choose from
	 */
	
	public void displayOptions(){
		System.out.println("Enter 1 to launch as master server");
		System.out.println("Enter 2 to launch as chunkserver");
		System.out.println("Enter 3 to launch as client");
	}
	
	/*
	 * This method is used to take input from user
	 */
	
	public int input(){
		int userInputOption = 0;
		
		displayOptions();
		
		try {
			sc = new Scanner(System.in);
			userInputOption = sc.nextInt();
		} catch (Exception e) {
			System.err.println("Please enter a valid option!!!");
		}
		
		return userInputOption;

	}
	
	/*
	 * Main method
	 */
	public static void main(String[] args) {
		Sdfs obj;
		int userInputOption;
		MasterServer ms;
		Chunkserver cs;
		Client c;
		
		obj = new Sdfs();
		
		do{
			// take input from client
			userInputOption = obj.input();
		} while(userInputOption == 0);
		
		if(userInputOption == 1){
			ms = new MasterServer();
			ms.initialise();
		} else if(userInputOption == 2){
			cs = new Chunkserver();
			cs.initialise();
		} else if(userInputOption == 3){
			c = new Client();
			c.initialise();
		}
			
	}

}
