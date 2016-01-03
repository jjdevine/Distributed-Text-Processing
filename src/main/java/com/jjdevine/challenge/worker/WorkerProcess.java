package com.jjdevine.challenge.worker;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.jjdevine.challenge.AppData;
import com.jjdevine.challenge.Logger;
import com.jjdevine.challenge.ProcessStatus;
import com.jjdevine.challenge.dao.ChallengeDAO;
import com.jjdevine.challenge.job.JobType;
import com.mongodb.client.FindIterable;

/**
 * Process class that is used for processing registered as worker nodes.
 * @author Jonathan
 *
 */
public class WorkerProcess {
	
	/**
	 * Core application configuration data.
	 */
	private AppData appData;
	
	/**
	 * DAO to access the persistent store.
	 */
	private ChallengeDAO challengeDAO;
	
	/**
	 * Reader that maintains an open connection to the file and is to be re-used for efficiency purposes.
	 */
	private BufferedReader reader;
	
	/**
	 * The nodeId of this worker.
	 * (Not currently required as instance variable however may be useful for future development)
	 */
	private String nodeId;
	
	/**
	 * Current line number of the BufferedReader.
	 */
	private long currentLineNumber = 1; //first line is 1 not 0

	/**
	 * Public Constructor.
	 * @param appData Core applciation config data.
	 * @param challengeDAO DAO to access the persistent store.
	 */
	public WorkerProcess(AppData appData, ChallengeDAO challengeDAO) {
		this.appData = appData;
		this.challengeDAO = challengeDAO;
	}
	
	/**
	 * Main processing method for worker nodes.
	 */
	public void process() {
		/*
		 * register as new node
		 */
		
		String nodeId = challengeDAO.registerAsWorker();
		this.nodeId = nodeId;
		
		Logger.log("New worker node - <" + nodeId + ">", nodeId);
		Logger.log(appData, nodeId);
		
		/*
		 * Wait for jobs 
		 */
		
		boolean processFinished = !ProcessStatus.ACTIVE.equals(challengeDAO.queryProcessStatus());
		
		try {
			while(!processFinished) {
				Document workerDocument = challengeDAO.getWorker(nodeId);
				String workerStatus = workerDocument.getString("status");
				
				if(workerStatus == null) {
					throw new RuntimeException("Unable to ascertain worker status");
				}
				
				switch(workerStatus) {
				case WorkerStatus.JOB_PENDING:
					challengeDAO.updateWorkerStatus(nodeId, WorkerStatus.WORKING);
					String jobId = workerDocument.getString("jobId");
					String jobType = workerDocument.getString("jobType");
					try {
						if(JobType.TEXT_PROCESSING.equals(jobType)) {
							processTextProcessingJob(jobId);
						} else if(JobType.AMALGAMATION.equals(jobType)){
							processAmalgamationJob(jobId);
						} else {
							throw new RuntimeException("Unrecognised job type <" + jobType + ">");
						}
						
					} catch (IOException e) {
						throw new RuntimeException("Unable to process job", e);
					}
					challengeDAO.updateWorkerStatus(nodeId, WorkerStatus.IDLE);
					break;
				case WorkerStatus.TIMED_OUT:
					//re-register to accept new jobs:
					nodeId = challengeDAO.registerAsWorker();
					this.nodeId = nodeId;
					break;
				}
				
				try {
					Thread.sleep(1000); //sleep before repolling
				} catch (InterruptedException e) {
					throw new RuntimeException("Unable to sleep before repolling", e);
				}
				
				processFinished = !ProcessStatus.ACTIVE.equals(challengeDAO.queryProcessStatus());
			}
		} finally {
			try {
				if(reader != null) {
					reader.close();
				}
			} catch(IOException ex) {
				throw new RuntimeException("Could not close reader.", ex);
			}
		}
		
		Logger.log("Process complete, exiting", nodeId);
	}
	
	/**
	 * Process a text processing job.
	 * @param jobId the id of the job.
	 * @throws IOException if a problem is encountered processing the file.
	 */
	private void processTextProcessingJob(String jobId) throws IOException {
		Map<String, Long> wordCounts = new HashMap<>();
		Document job = challengeDAO.getTextProcessingJob(jobId);
		long firstLine = job.getLong("firstLine");
		long lastLine = job.getLong("lastLine");
		
		Logger.log("Beginning text processing job <" + jobId + "> starting at line <" + firstLine + "> ending at line <" + lastLine + ">", nodeId);
		
		if(reader == null) {
			//first job, need to set up reader
			try {
				Logger.log("opening reader", nodeId);
				reader = Files.newBufferedReader(Paths.get(appData.getSourceLocation()));
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Could not find source file", e);
			} 
		}
		
		advanceToLine(firstLine);

		String[] words = null;
		while(currentLineNumber <= lastLine) {
			//don't bother with error checking? as this will slow down processing - runtime error here would indicate programming bug elsewhere.
			words = reader.readLine().split("\\b");
			
			currentLineNumber++;
			updateWordCounts(wordCounts, words);
		}

		challengeDAO.completeTextProcessingJob(jobId, wordCounts);
		Logger.log("Completed text processing job <" + jobId + ">", nodeId);
	}

	/**
	 * Update the word counts based on newly read works.
	 * @param wordCounts Map of words to number of occurances.
	 * @param words an array of newly read words to process.
	 */
	private void updateWordCounts(Map<String, Long> wordCounts, String[] words) {
		for(String word: words) {
			if(word.length() == 0
					|| !isLetter(word.charAt(0))) {
				continue;
			}
			
			word = word.toLowerCase(); //all words to be treated as lower case
			
			if(wordCounts.keySet().contains(word)) {
				wordCounts.put(word, wordCounts.get(word)+1);
			} else {
				wordCounts.put(word, 1L);
			}
			
		}
	}

	/**
	 * Hashset of valid letters - words not starting with these letters are ignored.
	 * (note: added because Mongo struggled to cope with words starting with odd characters)
	 */
	static Set<Character> validLetters = new HashSet<>();
	
	static {
		validLetters.add('A');
		validLetters.add('B');
		validLetters.add('C');
		validLetters.add('D');
		validLetters.add('E');
		validLetters.add('F');
		validLetters.add('G');
		validLetters.add('H');
		validLetters.add('I');
		validLetters.add('J');
		validLetters.add('K');
		validLetters.add('L');
		validLetters.add('M');
		validLetters.add('N');
		validLetters.add('O');
		validLetters.add('P');
		validLetters.add('Q');
		validLetters.add('R');
		validLetters.add('S');
		validLetters.add('T');
		validLetters.add('U');
		validLetters.add('V');
		validLetters.add('W');
		validLetters.add('X');
		validLetters.add('Y');
		validLetters.add('Z');
		validLetters.add('a');
		validLetters.add('b');
		validLetters.add('c');
		validLetters.add('d');
		validLetters.add('e');
		validLetters.add('f');
		validLetters.add('g');
		validLetters.add('h');
		validLetters.add('i');
		validLetters.add('j');
		validLetters.add('k');
		validLetters.add('l');
		validLetters.add('m');
		validLetters.add('n');
		validLetters.add('o');
		validLetters.add('p');
		validLetters.add('q');
		validLetters.add('r');
		validLetters.add('s');
		validLetters.add('t');
		validLetters.add('u');
		validLetters.add('v');
		validLetters.add('w');
		validLetters.add('x');
		validLetters.add('y');
		validLetters.add('z');
	}
	
	/**
	 * Ascertain if a character is a valid letter.
	 * @param c the chatacter to check.
	 * @return true if the provided character is a valid letter, otherwise false.
	 */
	private boolean isLetter(char c) {
		return validLetters.contains(c);
	}
	
	/**
	 * Advance the buffered reader to a line number within the source file.
	 * @param targetLineNumber The target line number.
	 * @throws IOException If an error occurs reading the file.
	 */
	private void advanceToLine(long targetLineNumber) throws IOException {
		while(currentLineNumber < targetLineNumber) {
			reader.readLine();
			currentLineNumber++;
		}
	}
	
	/**
	 * Process an amalgamation job.
	 * @param jobId the job id - in practice the prefix letter (eg 'a', 'aa', 'ab' etc.)
	 */
	private void processAmalgamationJob(String jobId) {
		Logger.log("Processing amalgamation for <" + jobId + ">", nodeId);
		
		FindIterable<Document> docsToAmalgamate = challengeDAO.getTextProcessingJobResultsByPrefix(jobId);
		
		Iterator<Document> i = docsToAmalgamate.iterator();
		Map<String, Long> wordCountTotals = new HashMap<>();
		
		while(i.hasNext()) {
			Document nextDoc = i.next();
			Document wordCounts = (Document)nextDoc.get("wordCounts");
			
			//iterate over words, updating or 
			Set<String> keys = wordCounts.keySet();
			for(String word: keys) {
				updateWordTotals(wordCountTotals, word, wordCounts.getLong(word));
			}
		}
		
		challengeDAO.completeAmalgamationJob(jobId, wordCountTotals);
	}
	
	/**
	 * Update a map of word counts based on the newly provided word and number of instances found.
	 * @param wordCountTotals Map to update.
	 * @param word the word newly identified.
	 * @param wordCount The number of occurrences of the word.
	 */
	private void updateWordTotals(Map<String, Long> wordCountTotals, String word, long wordCount) {
		if(wordCountTotals.keySet().contains(word)) {
			wordCountTotals.put(word, wordCountTotals.get(word)+wordCount);
		} else {
			wordCountTotals.put(word, wordCount);
		}
	}
}
