package com.jjdevine.challenge.worker;

/**
 * Constants class enumerating possible statuses of worker nodes.
 * @author Jonathan
 *
 */
public final class WorkerStatus {

	/**
	 * private constructor to prevent instantiation
	 */
	private WorkerStatus() {}
	
	/**
	 * newly registered worker
	 */
	public static final String NEW = "NEW"; 
	
	/**
	 * currently working on a job
	 */
	public static final String WORKING = "WORKING"; 
	
	/**
	 * job finished, awaiting a new one
	 */
	public static final String IDLE = "IDLE"; 
	
	/**
	 * job assigned but not yet started
	 */
	public static final String JOB_PENDING = "JOB_PENDING";
	
	/**
	 * Worker is considered timed out and will have to re-register to take any further part in the process.
	 */
	public static final String TIMED_OUT = "TIMED_OUT";
}
