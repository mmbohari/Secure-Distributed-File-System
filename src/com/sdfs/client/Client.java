package com.sdfs.client;

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
	
	/*
	 * This method is used to initialize the client
	 */
	public void initialise(){
		displayLines();
		// take input
		input();
		displayLines();
	}
	
	/*
	 * This method is used to display possible operations
	 * that a user can perform
	 */
	public void displayOperations(){
		System.out.println("1. Register");
		System.out.println("2. Login");
		System.out.println("3. Create a file");
		System.out.println("4. Append data to an existing file");
		System.out.println("5. Read a file");
		System.out.println("6. Delete a file");
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
	 * This method is used to display lines on console 
	 * for making the screen elegant 
	 */
	public void displayLines(){
		System.out.println("=================================>>");
	}
	
}
