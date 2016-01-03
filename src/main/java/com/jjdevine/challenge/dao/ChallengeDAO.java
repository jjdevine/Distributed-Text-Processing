package com.jjdevine.challenge.dao;

import java.util.Map;

import org.bson.Document;

import com.mongodb.client.FindIterable;

/**
 * DAO for storing and retrieving data related to the word count process.
 * @author Jonathan
 *
 */
public interface ChallengeDAO {
	
	/**
	 * Register this node as the control process.
	 * @return the registered. nodeId
	 */
	String registerAsController();
	
	/**
	 * Check if the control process is still running.
	 * @return true if the control process is active, otherwise false.
	 */
	boolean isControllerActive();
	
	/**
	 * Set the status of the controller with the designated id to the status given.
	 * @param nodeId The controller to edit.
	 * @param status The status to set.
	 */
	void setControllerStatus(String nodeId, String status);
	
	/**
	 * Register this node as a worker.
	 * @return the registered nodeId.
	 */
	String registerAsWorker();
	
	/**
	 * Gets a list of all workers registered to accept jobs.
	 * @return A FindIterable<Document> of all the registered workers. 
	 */
	FindIterable<Document> getWorkers();
	
	/**
	 * Clear all collections associated with this process (and lose any data).
	 * This should be run to reset the process ready to run anew.
	 */
	void clearAllCollections();
	
	/**
	 * Create a text processing job.
	 * @param firstLine The line number where the job should begin.
	 * @param lastLine The line number where the job should terminate.
	 * @return The jobId of the created job.
	 */
	String createTextProcessingJob(long firstLine, long lastLine);
	
	/**
	 * Get the text processing job with the provided id.
	 * @param jobId the jobId to match.
	 * @return The Job in the form of a Document.
	 */
	Document getTextProcessingJob(String jobId);
	
	/**
	 * Update a text processing job to be complete and store the results.
	 * @param jobId The jobId to mark completed.
	 * @param wordCounts The results of the job to be stored.
	 */
	void completeTextProcessingJob(String jobId, Map<String, Long> wordCounts);
	
	/**
	 * Update an amalgamation job to be complete and store the results.
	 * @param prefix The prefix this job related to.
	 * @param wordCountTotals The results of the job to be stored.
	 */
	void completeAmalgamationJob(String prefix, Map<String, Long> wordCountTotals);
	
	/**
	 * Check if all text processing jobs are completed.
	 * @return true if all the jobs are complete, otherwise false.
	 */
	boolean allTextProcessingJobsComplete();
	
	/**
	 * Check if all amalgamation jobs are completed.
	 * @return true if all the jobs are complete, otherwise false.
	 */
	boolean allAmalgamationJobsComplete();
	
	/**
	 * Assign a job to a worker node.
	 * @param workerId The id of the worker.
	 * @param jobId The id of the job.
	 * @param type The type of job (see JobType class)
	 */
	void assignJobToWorker(String workerId, String jobId, String type);
	
	/**
	 * Create all the necessary amalgamation jobs ready to be assigned.
	 */
	void createAmalgamationJobs();
	
	/**
	 * Mark the overall process as having begun.
	 */
	void startProcess();
	
	/**
	 * Update the overall process state.
	 * @param processState the process state to set
	 */
	void setProcessState(String processState);
	
	/**
	 * Obtain the current process state.
	 * @return The current process state.
	 */
	String queryProcessStatus();
	
	/**
	 * Get a specific worker node.
	 * @param workerId The worker node id.
	 * @return A Document of information related to the worker.
	 */
	Document getWorker(String workerId);
	
	/**
	 * update the status of a worker node.
	 * @param workerId the id of the worker to update.
	 * @param status the status to set.
	 */
	void updateWorkerStatus(String workerId, String status);
	
	/**
	 * Return text processing job results relating to a given prefix.
	 * @param prefix The prefix for which job results are required.
	 * @return A FindIterable<Document> of the job results with a matching prefix.
	 */
	FindIterable<Document> getTextProcessingJobResultsByPrefix(String prefix);

	/**
	 * Get the next unassigned amalgamation job and move it to 'pending' status.
	 * @return The prefix of the amalgamation job.
	 */
	String getNextUnassignedAmalgamationJob();
	
}
