package com.jjdevine.challenge;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.jjdevine.challenge.controller.ControllerProcess;
import com.jjdevine.challenge.dao.ChallengeDAO;
import com.jjdevine.challenge.dao.ChallengeDAOImpl;
import com.jjdevine.challenge.worker.WorkerProcess;

/**
 * Main class from which the application is launched.
 * @author Jonathan
 *
 */
public class App 
{
	/**
	 * Number of worker threads to use.
	 */
	private static final int NUM_THREADS = 3;
	
	/**
	 * Core configuration data for the application.
	 */
	private AppData appData;
	
	/**
	 * DAO used to communicate with persistent store.
	 */
	private ChallengeDAO challengeDAO;
	
	/**
	 * Public constructor.
	 * @param appData configuration data
	 */
	public App(AppData appData) {
		this.appData = appData;
		challengeDAO = new ChallengeDAOImpl(appData);
	}
	
	/**
	 * Initialise application - ascertain whether application should run as a controller (ie - is this the first instance)
	 * or a worker (2nd instance or later)
	 */
	public void initialise() {
		try {
			if(shouldRunAsControlProcess()) {
				challengeDAO.clearAllCollections(); //reset all collections
				new ControllerProcess(appData, challengeDAO).process();
			} else {
				Logger.log("Creating " + NUM_THREADS + " worker threads.", "n/a");
				//spawn a number of worker threads
				
				List<Thread> threads = new ArrayList<>();
				
				for(int count=0; count < NUM_THREADS; count++) {
					
					
					Thread t = new Thread(new Runnable() {
						
						@Override
						public void run() {
							new WorkerProcess(appData, challengeDAO).process();
						}
					});
					threads.add(t);
					t.start();
				}
				
				for(Thread thread: threads) {
					try {
						thread.join();
					} catch (InterruptedException e) {
						throw new RuntimeException("Error joining thread", e);
					}
				}
			}
		} finally {
			Logger.log("closing connections", "n/a");
			challengeDAO.closeConnections();
		}
	}

	/**
	 * Ascertains if application should run as the control process.
	 * @return true if application should run as control, otherwise false.
	 */
	private boolean shouldRunAsControlProcess() {
		return !challengeDAO.isControllerActive();
	}
	
	/**
	 * Main method to launch the appliction.
	 * @param args applciation arguments
	 * @throws Exception if the application fails in a manner that cannot be recovered.
	 */
    public static void main( String[] args ) throws Exception {
    	new App(parseConfiguration(args)).initialise();
    }
    
    /**
     * Parses the arguments passed to the application on startup.
     * @param args the application arguments.
     * @return An AppData object containing application configuration.
     */
    private static AppData parseConfiguration(String[] args) {

    	String currentArgName = null;
    	AppData appData = new AppData();
    	for(int index=0; index<args.length; index++) {
    		if(index == 0 || index % 2 == 0) { //this is an arg name
    			currentArgName = args[index];
    		} else { //this is an arg value
    			String argValue = args[index];
    			switch(currentArgName) {
    			case "-source":
    				if (!(new File(argValue)).exists()) {
    					throw new IllegalStateException("File <" + argValue + "> does not exist");
    				}
    				appData.setSourceLocation(argValue);
    				break;
    			case "-mongo":
    				appData.setMongoHost(argValue);
    				break;
    			case "-id": //may be supplied but will be ignored
    				break;
    			default:
    				throw new IllegalArgumentException("<" + currentArgName + "> is not a valid switch");
    			}
    		}	
    	}
    	
    	if(appData.getSourceLocation() == null) {
    		throw new IllegalArgumentException("-source must be set");
    	}
    	
    	if(appData.getMongoHost() == null) {
    		throw new IllegalArgumentException("-mongo must be set");
    	}
    	return appData;
    }
}
 