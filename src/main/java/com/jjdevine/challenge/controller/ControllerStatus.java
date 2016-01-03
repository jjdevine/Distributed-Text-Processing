package com.jjdevine.challenge.controller;

/**
 * Constants class enumerative possible states of the controller process. 
 * @author Jonathan
 *
 */
public class ControllerStatus {

	/**
	 * Private constructor - this is a constants class.
	 */
	private ControllerStatus() {}
	
	/**
	 * State when the controller process is still running.
	 */
	public static final String ACTIVE = "active";
	
	/**
	 * State when the controller process has terminated.
	 */
	public static final String FINISHED = "finished";
}


