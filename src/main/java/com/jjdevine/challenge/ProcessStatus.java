package com.jjdevine.challenge;

/**
 * Constant class containing possible statuses of the overall process.
 * @author Jonathan
 *
 */
public class ProcessStatus {
	
	/**
	 * Process state when the process is still in progress.
	 */
	public static final String ACTIVE = "ACTIVE";
	
	/**
	 * Process state when the process has completed.
	 */
	public static final String COMPLETE = "COMPLETE";
	
	/**
	 * Process state when the process has terminated due to an error.
	 */
	public static final String ERROR = "ERROR";
}

