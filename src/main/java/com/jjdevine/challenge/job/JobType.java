package com.jjdevine.challenge.job;

/**
 * Constants class enumerating possible job types.
 * @author Jonathan
 *
 */
public class JobType {

	/**
	 * private constructor; constants class.
	 */
	private JobType() {}
	
	/**
	 * A text processing job that counts words for a segment of text.
	 */
	public static final String TEXT_PROCESSING = "TEXT_PROCESSING";
	
	/**
	 * An amalgamation job that merges the results of completed text processing jobs.
	 */
	public static final String AMALGAMATION = "AMALGAMATION";
}
