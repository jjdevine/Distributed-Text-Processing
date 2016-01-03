package com.jjdevine.challenge.controller;

/**
 * Class modelling state pertinent to the controller process.
 * @author Jonathan
 *
 */
public class ControllerState {

	/**
	 * total lines in target file.
	 */
	public long totalLines = 0;
	
	/**
	 * flag to indicate if all lines have been counted (ie, is totalLines going to be updated)
	 */
	private boolean lineCountFinished = false;
	
	/**
	 * if true, indicates an error in the line counting process
	 */
	private boolean lineCountError = false;
	
	/**
	 * The last line assigned to a job - ie any subsequent lines can be assigned out as new jobs
	 */
	private long lastLineAssignedToTextProcessingJob = 0;

	/**
	 * The node id of the controller.
	 */
	private String nodeId;

	/**
	 * @return the totalLines
	 */
	public long getTotalLines() {
		return totalLines;
	}

	/**
	 * @param totalLines the totalLines to set
	 */
	public void setTotalLines(long totalLines) {
		this.totalLines = totalLines;
	}

	/**
	 * @return the lineCountFinished
	 */
	public boolean isLineCountFinished() {
		return lineCountFinished;
	}

	/**
	 * @param lineCountFinished the lineCountFinished to set
	 */
	public void setLineCountFinished(boolean lineCountFinished) {
		this.lineCountFinished = lineCountFinished;
	}

	/**
	 * @return the lineCountError
	 */
	public boolean isLineCountError() {
		return lineCountError;
	}

	/**
	 * @param lineCountError the lineCountError to set
	 */
	public void setLineCountError(boolean lineCountError) {
		this.lineCountError = lineCountError;
	}

	/**
	 * @return the lastLineAssignedToTextProcessingJob
	 */
	public long getLastLineAssignedToTextProcessingJob() {
		return lastLineAssignedToTextProcessingJob;
	}

	/**
	 * @param lastLineAssignedToTextProcessingJob the lastLineAssignedToJob to set
	 */
	public void setLastLineAssignedToTextProcessingJob(long lastLineAssignedToTextProcessingJob) {
		this.lastLineAssignedToTextProcessingJob = lastLineAssignedToTextProcessingJob;
	}

	/**
	 * @return the nodeId
	 */
	public String getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId the nodeId to set
	 */
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ControllerState [totalLines=" + totalLines + ", lineCountFinished=" + lineCountFinished
				+ ", lineCountError=" + lineCountError + ", lastLineAssignedToTextProcessingJob="
				+ lastLineAssignedToTextProcessingJob + ", nodeId=" + nodeId + "]";
	}
}
