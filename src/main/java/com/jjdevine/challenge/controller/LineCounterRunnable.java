package com.jjdevine.challenge.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.jjdevine.challenge.AppData;
import com.jjdevine.challenge.Logger;

/**
 * Runnable class used to count the lines of the target text file.
 * @author Jonathan
 *
 */
public class LineCounterRunnable implements Runnable {

	/**
	 * State of the controller process.
	 */
	private ControllerState state;
	
	/**
	 * Core application data.
	 */
	private AppData appData;
	
	/**
	 * public constructor.
	 * @param state The controller state object.
	 * @param appData The core application data object.
	 */
	public LineCounterRunnable(ControllerState state, AppData appData) {
		this.state = state;
		this.appData = appData;
	}
	
	/**
	 * Counts the lines of the target file and updates the controller state with the number of lines identified in real-time.
	 */
	@Override
	public void run() {
		
		try (BufferedReader reader = new BufferedReader(new FileReader(appData.getSourceLocation()))) {

			while(reader.readLine() != null) {
				state.totalLines++; //use direct access for speed
			}
			
			reader.close();
			
			state.setLineCountFinished(true);
			Logger.log("Line count finished, total lines to process = " + state.totalLines, state.getNodeId());
		} catch (IOException e) {
			state.setLineCountError(true);
			//TODO - handle this error condition
		}
	}

}
