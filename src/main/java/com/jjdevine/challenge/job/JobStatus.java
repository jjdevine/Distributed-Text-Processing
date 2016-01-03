package com.jjdevine.challenge.job;

/**
 * Constants class enumerating possible job statuses.
 * @author Jonathan
 *
 */
public class JobStatus {
	
	/**
	 * private constructor; constants class.
	 */
	private JobStatus() {}
	
	/**
	 * A job that is not assigned to a worker.
	 */
	public static final String UNASSIGNED = "UNASSIGNED";
	
	/**
	 * A job that is incomplete.
	 */
	public static final String PENDING = "PENDING";
	
	/**
	 * A job that has been completed.
	 */
	public static final String COMPLETE = "COMPLETE";
}
