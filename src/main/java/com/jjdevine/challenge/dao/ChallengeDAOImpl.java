package com.jjdevine.challenge.dao;

import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.jjdevine.challenge.AppData;
import com.jjdevine.challenge.ProcessStatus;
import com.jjdevine.challenge.controller.ControllerStatus;
import com.jjdevine.challenge.job.JobStatus;
import com.jjdevine.challenge.worker.WorkerStatus;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

/**
 * Implementation of the DAO interface, uses a MongoDB to store information.
 * @author Jonathan
 *
 */
public class ChallengeDAOImpl implements ChallengeDAO {
	
	/**
	 * The Mongo client.
	 */
	private MongoClient client;
	
	/**
	 * The Mongo DB.
	 */
	private MongoDatabase db;
	
	/**
	 * The collection of registered controller nodes.
	 */
	private static final String COLLECTION_CONTROLLER = "controller";
	
	/**
	 * The collection of registered worker nodes.
	 */
	private static final String COLLECTION_WORKERS = "workers";
	
	/**
	 * The collection of text processing jobs.
	 */
	private static final String COLLECTION_TEXT_PROCESSING_JOBS = "jobs";
	
	/**
	 * The collection of text processing job results.
	 */
	private static final String COLLECTION_TEXT_PROCESSING_JOB_RESULTS = "job_results";
	
	/**
	 * The collection of amalgamation jobs.
	 */
	private static final String COLLECTION_AMALGAMATION_JOBS = "amalgamation_jobs";
	
	/**
	 * The collection of data related to the overall process.
	 */
	private static final String COLLECTION_PROCESS = "process";
	
	/**
	 * The collection where the final results are stored.
	 */
	private static final String COLLECTION_FINAL_COUNTS = "final_word_counts";
	
	/**
	 * The Database to use
	 */
	private static final String DATABASE = "challenge";

	/**
	 * Public constructor.
	 * @param appData Core application configuration data.
	 */
	public ChallengeDAOImpl(AppData appData) {
		client = new MongoClient(appData.getMongoHost());
		db = client.getDatabase(DATABASE);
	}

	@Override
	public String registerAsController() {
		//clear down from previous runs
		db.getCollection(COLLECTION_CONTROLLER).deleteMany(new Document());
		
		Document doc = new Document()
				.append("status", ControllerStatus.ACTIVE)
				.append("timestamp", new Date());
		
		db.getCollection(COLLECTION_CONTROLLER).insertOne(doc);
		
		return doc.get("_id").toString();
	}
	
	@Override
	public void clearAllCollections() {
		db.getCollection(COLLECTION_CONTROLLER).deleteMany(new Document());
		db.getCollection(COLLECTION_WORKERS).deleteMany(new Document());
		db.getCollection(COLLECTION_TEXT_PROCESSING_JOBS).deleteMany(new Document());
		db.getCollection(COLLECTION_TEXT_PROCESSING_JOB_RESULTS).deleteMany(new Document());
		db.getCollection(COLLECTION_AMALGAMATION_JOBS).deleteMany(new Document());
		db.getCollection(COLLECTION_PROCESS).deleteMany(new Document());
		db.getCollection(COLLECTION_FINAL_COUNTS).deleteMany(new Document());
	}
	
	@Override
	public boolean isControllerActive() {
		FindIterable<Document> iterable = db.getCollection(COLLECTION_CONTROLLER).find(new Document("status", "active"));
		return iterable.first() != null;
	}
	
	@Override
	public void setControllerStatus(String nodeId, String status) {
		db.getCollection(COLLECTION_CONTROLLER).updateOne(
				new Document("_id", new ObjectId(nodeId)), 
				new Document("$set", 
						new Document("status", status)));
	}
	
	@Override
	public String registerAsWorker() {
		Document doc = new Document()
				.append("status", WorkerStatus.NEW);
		db.getCollection(COLLECTION_WORKERS).insertOne(doc);
		return doc.get("_id").toString();
	}
	
	@Override
	public FindIterable<Document> getWorkers() {
		return db.getCollection(COLLECTION_WORKERS).find();
	}
	
	@Override
	public String createTextProcessingJob(long firstLine, long lastLine) {
		Document doc = new Document()
				.append("firstLine", firstLine)
				.append("lastLine", lastLine)
				.append("status", JobStatus.PENDING);
		
		db.getCollection(COLLECTION_TEXT_PROCESSING_JOBS).insertOne(doc);
		
		return doc.get("_id").toString();
	}
	
	@Override
	public void assignJobToWorker(String workerId, String jobId, String type) {
		db.getCollection(COLLECTION_WORKERS).updateOne(
				new Document("_id", new ObjectId(workerId)), 
				new Document("$set", 
						new Document("jobId", jobId)
						.append("status", WorkerStatus.JOB_PENDING)
						.append("jobType", type)));
	}
	
	@Override
	public void startProcess() {
		db.getCollection(COLLECTION_PROCESS).deleteMany(new Document()); //clear down any old processes
		db.getCollection(COLLECTION_PROCESS).insertOne(new Document("status", ProcessStatus.ACTIVE));
	}
	
	@Override
	public void setProcessState(String processState) {
		db.getCollection(COLLECTION_PROCESS).updateOne(new Document(), 
				new Document("$set", 
						new Document("status", processState)));
	}
	
	@Override
	public String queryProcessStatus() {
		Document doc = db.getCollection(COLLECTION_PROCESS).find(new Document()).first();
		if(doc != null) {
			return doc.getString("status");
		} else {
			return null;
		}
		
	}
	
	@Override
	public Document getWorker(String workerId) {
		Document doc = db.getCollection(COLLECTION_WORKERS).find(
				new Document("_id", new ObjectId(workerId))).first();
		
		return doc;
	}
	
	@Override
	public void updateWorkerStatus(String workerId, String status) {
		db.getCollection(COLLECTION_WORKERS).updateOne(
				new Document("_id", new ObjectId(workerId)), 
				new Document("$set", 
						new Document("status", status)));
	}

	@Override
	public Document getTextProcessingJob(String jobId) {
		return db.getCollection(COLLECTION_TEXT_PROCESSING_JOBS).find(
				new Document("_id", new ObjectId(jobId))).first();
	}

	@Override
	public void completeTextProcessingJob(String jobId, Map<String, Long> wordCounts) {

		if(wordCounts.size() > 0) {
			/*
			 * Split results based on first letters/prefix of words. Have a document for 'a' 'b' etc as well as 'aa', 'ab' 
			 * (ie use all possible 1 and 2 letter prefixes
			 */
			
			//sort keys alphabetically into an array
			String[] keys = new String[wordCounts.keySet().size()];
			wordCounts.keySet().toArray(keys);
			Arrays.sort(keys);
			
			//list of result documents
			List<Document> jobResultDocuments = new ArrayList<>();
			
			//the document currently being worked on
			Document currentJobResultDocument = new Document()
					.append("jobId", jobId);
			jobResultDocuments.add(currentJobResultDocument);

			//record the prefix (first letter) used in this document
			String currentPrefix = getPrefix(keys[0]);
			currentJobResultDocument.append("prefix", currentPrefix);
			
			//the word block of the document currently being worked on
			Document currentWordBlock = new Document();
			currentJobResultDocument.append("wordCounts", currentWordBlock);
			for(String word: keys) {
				if(!currentPrefix.equals(getPrefix(word))) {
					/*
					 * prefix has changed, create a new document for new start letter
					 */
					currentPrefix = getPrefix(word);
					
					currentJobResultDocument = new Document()
							.append("jobId", jobId)
							.append("prefix", currentPrefix);
					jobResultDocuments.add(currentJobResultDocument);
					
					currentWordBlock = new Document();
					currentJobResultDocument.append("wordCounts", currentWordBlock);
				}
				currentWordBlock.append(word, wordCounts.get(word));
			}

			db.getCollection(COLLECTION_TEXT_PROCESSING_JOB_RESULTS).insertMany(jobResultDocuments);
		}

		db.getCollection(COLLECTION_TEXT_PROCESSING_JOBS).updateOne(
				new Document("_id", new ObjectId(jobId)), 
				new Document("$set", 
						new Document("status", JobStatus.COMPLETE)));
	}
	
	/**
	 * Return the prefix for the string, using the first two characters. If the string has a length less than two, use the string as is.
	 * @param str The string whose prefix is required.
	 * @return The prefix
	 */
	private String getPrefix(String str) {
		if(str.length() <= 1) {
			return str;
		} else {
			return str.substring(0, 2);
		}
	}
	
	@Override
	public void completeAmalgamationJob(String prefix, Map<String, Long> wordCountTotals) {
		
		//sort keys (words) alphabetically into an array
		String[] keys = new String[wordCountTotals.keySet().size()];
		wordCountTotals.keySet().toArray(keys);
		Arrays.sort(keys);
		
		Document resultDoc = new Document()
				.append("prefix", prefix);
		
		Document wordCounts = new Document();
		resultDoc.append("wordCounts", wordCounts);
		
		for(String word: keys) {
			wordCounts.append(word, wordCountTotals.get(word));
		}
		
		db.getCollection(COLLECTION_FINAL_COUNTS).insertOne(resultDoc);
		
		db.getCollection(COLLECTION_AMALGAMATION_JOBS).updateOne(
				new Document("prefix", prefix),
				new Document("$set",
						new Document("status", JobStatus.COMPLETE)));

	}
	
	@Override
	public boolean allTextProcessingJobsComplete() {
		FindIterable<Document> result = db.getCollection(COLLECTION_TEXT_PROCESSING_JOBS).find(ne("status", JobStatus.COMPLETE));
		return result.first() == null;
	}
	
	@Override
	public boolean allAmalgamationJobsComplete() {
		FindIterable<Document> result = db.getCollection(COLLECTION_AMALGAMATION_JOBS).find(ne("status", JobStatus.COMPLETE));
		return result.first() == null;
	}
	
	@Override
	public void createAmalgamationJobs() {
		List<Document> jobs = new ArrayList<>();
		
		Document job = null;
		
		for(char c1 = 'a'; c1<='z'; c1++) {
			//one letter prefixes
			
			job = new Document()
					.append("prefix", c1)
					.append("status", JobStatus.UNASSIGNED);
			jobs.add(job);
			
			for(char c2 = 'a'; c2<='z'; c2++) {
				//two letter prefixes
				StringBuilder prefix = new StringBuilder().append(c1).append(c2);
				job = new Document()
						.append("prefix", prefix.toString())
						.append("status", JobStatus.UNASSIGNED);
				jobs.add(job);
			}
		}
		
		db.getCollection(COLLECTION_AMALGAMATION_JOBS).insertMany(jobs);
	}
	
	@Override
	public String getNextUnassignedAmalgamationJob() {
		Document jobDoc = db.getCollection(COLLECTION_AMALGAMATION_JOBS).findOneAndUpdate(
				new Document("status", JobStatus.UNASSIGNED), 
				new Document("$set",
						new Document("status", JobStatus.PENDING)));
		
		if(jobDoc == null) {
			return null;
		}
		
		return jobDoc.getString("prefix");
	}
	
	@Override
	public FindIterable<Document> getTextProcessingJobResultsByPrefix(String prefix) {
		return db.getCollection(COLLECTION_TEXT_PROCESSING_JOB_RESULTS).find(
				new Document().append("prefix", prefix));
	}

	@Override
	public void closeConnections() {
		client.close();
	}
	
	
}
