package com.jjdevine.challenge.controller;

import java.util.Iterator;

import org.bson.Document;

import com.jjdevine.challenge.AppData;
import com.jjdevine.challenge.Logger;
import com.jjdevine.challenge.ProcessStatus;
import com.jjdevine.challenge.dao.ChallengeDAO;
import com.jjdevine.challenge.job.JobType;
import com.jjdevine.challenge.worker.WorkerStatus;
import com.mongodb.client.FindIterable;

/**
 * Process that is run when the application is used as a controller node.
 * @author Jonathan
 *
 */
public class ControllerProcess {

	/**
	 * Number of lines to process in each text processing job
	 */
	private static final int TEXT_PROCESSING_JOB_SIZE = 2_000_000;
	
	/**
	 * Core application configuration data.
	 */
	private AppData appData;
	
	/**
	 * DAO for accessing the persistent store.
	 */
	private ChallengeDAO challengeDAO;
	
	/**
	 * State pertinent to the controller process.
	 */
	private ControllerState controllerState;
	
	/**
	 * Public constructor.
	 * @param appData core application configuration data.
	 * @param challengeDAO DAO for accessing the datastore.
	 */
	public ControllerProcess(AppData appData, ChallengeDAO challengeDAO) {
		this.appData = appData;
		this.challengeDAO = challengeDAO;
		controllerState = new ControllerState();
	}
	
	/**
	 * Main method for running the controller process.
	 */
	public void process() {
		String nodeId = challengeDAO.registerAsController();
		controllerState.setNodeId(nodeId);
		
		//mark process as started
		challengeDAO.startProcess();
		
		Logger.log("Running as controller process. Awaiting available worker nodes.", nodeId);
		Logger.log(appData, nodeId);
		
		/*
		 *	Begin Line counter process 
		 */
		
		Runnable lineCounterRunnable = new LineCounterRunnable(controllerState, appData);
		Thread lineCounterThread = new Thread(lineCounterRunnable);
		lineCounterThread.start();
		
		/*
		 * Text processing jobs
		 * Poll state of workers - assign work to available workers
		 */
		
		boolean workFinished = false;
		
		Logger.log("Beginning text processing phase", nodeId);
		
		while(!workFinished) {
			//TODO - detect timeouts
			FindIterable<Document> workers = challengeDAO.getWorkers();
			Iterator<Document> workersIterator = workers.iterator();
			
			while (workersIterator.hasNext()) {
				
				Document doc = workersIterator.next();
				String status = doc.getString("status");
				
				switch(status) {
				case WorkerStatus.NEW:
				case WorkerStatus.IDLE:
					String jobId = getNextTextProcessingJob();
					if(jobId == null) { //no more jobs
						break;
					}
					String workerId = doc.get("_id").toString();
					challengeDAO.assignJobToWorker(doc.get("_id").toString(), jobId, JobType.TEXT_PROCESSING);
					Logger.log("Assigned job <" + jobId + "> to worker <" + workerId + ">", nodeId);
					break;
				}
			}
			
			/**
			 * If the line count process is finished, all lines have been assigned to a job
			 * and all jobs are marked as complete, the work is complete
			 */
			if(controllerState.isLineCountFinished() 
					&& controllerState.getLastLineAssignedToTextProcessingJob() == controllerState.totalLines
					&& challengeDAO.allTextProcessingJobsComplete()) {
				workFinished = true;
			}
			
			try {
				Thread.sleep(2000); //wait 2 seconds before repolling
			} catch (InterruptedException e) {
				throw new RuntimeException("Unable to sleep during polling process.", e);
			} 
		}
		
		Logger.log("Creating amalgamation jobs", nodeId);
		
		/*
		 * Now process Amalgamation jobs
		 */
		
		challengeDAO.createAmalgamationJobs();
		
		boolean amalgamationFinished = false;
		
		Logger.log("Beginning amalgamation phase", nodeId);
		
		while(!amalgamationFinished) {
			FindIterable<Document> workers = challengeDAO.getWorkers();
			Iterator<Document> workersIterator = workers.iterator();
			
			while (workersIterator.hasNext()) {
				
				Document doc = workersIterator.next();
				String status = doc.getString("status");
				
				switch(status) {
				case WorkerStatus.NEW:
				case WorkerStatus.IDLE:
					//create next amalgamation job
					String nextAmalgamationPrefix = getNextAmalgamationJob();
					
					if(nextAmalgamationPrefix == null) {
						//no jobs to process;
						break;
					} else {
						String workerId = doc.get("_id").toString();
						String jobId = nextAmalgamationPrefix.toString();
						challengeDAO.assignJobToWorker(doc.get("_id").toString(), nextAmalgamationPrefix.toString(), JobType.AMALGAMATION);
						Logger.log("Assigned amalgamation job <" + jobId + "> to worker <" + workerId + ">", nodeId);
					}
					break;
				}
			}
			
			amalgamationFinished = challengeDAO.allAmalgamationJobsComplete();
			
			try {
				Thread.sleep(500); //wait 0.5 seconds before repolling
			} catch (InterruptedException e) {
				throw new RuntimeException("Unable to sleep during polling process.", e);
			} 
		}
		
		challengeDAO.setProcessState(ProcessStatus.COMPLETE);
		challengeDAO.setControllerStatus(nodeId, ControllerStatus.FINISHED);
		Logger.log("Work Finished", nodeId);
	}
	
	/**
	 * Get the id of the next text processing job - either a new job or a previously timed out job.
	 * @return a job id, or null if no jobs remain
	 */
	private String getNextTextProcessingJob() {
		if(controllerState.isLineCountError()) {
			//TODO - throw better exception class
			throw new RuntimeException("Unable to count lines of file.");
		}
		
		boolean lineCountFinished = controllerState.isLineCountFinished();
		long lastLineProcessed = controllerState.getLastLineAssignedToTextProcessingJob();
			
		if(lineCountFinished && lastLineProcessed >= controllerState.totalLines) {
			//all lines have already been assigned out to jobs - check if any old jobs have timed out and reassigned
			//TODO - as per comment
			return null;
		}
		
		if(lastLineProcessed + TEXT_PROCESSING_JOB_SIZE < controllerState.totalLines) {
			//there are enough lines available to create a new job
			return createNewTextProcessingJob(lastLineProcessed + 1, lastLineProcessed + TEXT_PROCESSING_JOB_SIZE);
		} else {
			/*
			 * There are not enough lines to create a new full-size job - this could be because:
			 * a. We have reached the end of the file
			 * b. We have not reached the end of the file but the line reader has not finished counting the lines.
			 * 
			 * in scenario (a) we should create a job anyway, in scenario (b) we should wait until either the line reader reads
			 * enough lines to create a full size job or it finishes (hits EOF).
			 */
			
			if(controllerState.isLineCountFinished()) {
				//scenario (a)
				return createNewTextProcessingJob(lastLineProcessed + 1, controllerState.getTotalLines());
			} else {
				//scenario (b)
				//wait until either line count finishes or there are enough lines for a full size job, whichever happens first.
				while(true) {
					if(controllerState.isLineCountFinished() || lastLineProcessed + TEXT_PROCESSING_JOB_SIZE < controllerState.totalLines) {
						if(lastLineProcessed + TEXT_PROCESSING_JOB_SIZE < controllerState.totalLines) {
							return createNewTextProcessingJob(lastLineProcessed + 1, lastLineProcessed + TEXT_PROCESSING_JOB_SIZE);
						} else {
							//EOF was reached
							return createNewTextProcessingJob(lastLineProcessed + 1, controllerState.totalLines);
						}
					} else {
						try {
							Thread.sleep(1500); //wait before trying again
						} catch (InterruptedException e) {
							throw new RuntimeException("Unable to wait for line counter to finish", e);
						} 
					}
				}
			}
		}
	}
	
	/**
	 * Create a new text processing job that a worker node can pick up.
	 * @param firstLine the first line to be processed by the job
	 * @param lastLine the line where the job should terminate (inclusive)
	 * @return the job id.
	 */
	public String createNewTextProcessingJob(long firstLine, long lastLine) {
		String jobId = challengeDAO.createTextProcessingJob(firstLine, lastLine);
		controllerState.setLastLineAssignedToTextProcessingJob(lastLine);
		return jobId;
	}
	
	/**
	 * Get the next amalgamation job.
	 * @return the prefix char to process
	 */
	public String getNextAmalgamationJob() {
		return challengeDAO.getNextUnassignedAmalgamationJob();
		
		//TODO - check for timed out jobs
	}
}