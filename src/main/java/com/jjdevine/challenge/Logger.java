package com.jjdevine.challenge;

import java.util.Date;

/**
 * Utility class for writing log information. Currently just logs to console.
 * @author Jonathan
 *
 */
public class Logger {
	
	/**
	 * Write the provided message to the output.
	 * @param message the message to log.
	 */
	public static void log(Object message, String nodeId) {
		System.out.println("Node <" + nodeId + "> " + new Date() + " " + message);
	}
}
