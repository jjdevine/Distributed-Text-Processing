package com.jjdevine.challenge;

public class AppData {
	
	/**
	 * The location of the source text file.
	 */
	private String sourceLocation;
	
	/**
	 * The host string of the Mongo instance.
	 */
	private String mongoHost;
	
	/**
	 * The id of the Mongo instance to use.
	 */
	private String mongoId;

	/**
	 * @return the sourceLocation
	 */
	public String getSourceLocation() {
		return sourceLocation;
	}

	/**
	 * @param sourceLocation the sourceLocation to set
	 */
	public void setSourceLocation(String sourceLocation) {
		this.sourceLocation = sourceLocation;
	}

	/**
	 * @return the mongoHost
	 */
	public String getMongoHost() {
		return mongoHost;
	}

	/**
	 * @param mongoHost the mongoHost to set
	 */
	public void setMongoHost(String mongoHost) {
		this.mongoHost = mongoHost;
	}

	/**
	 * @return the mongoId
	 */
	public String getMongoId() {
		return mongoId;
	}

	/**
	 * @param mongoId the mongoId to set
	 */
	public void setMongoId(String mongoId) {
		this.mongoId = mongoId;
	}

	@Override
	public String toString() {
		return "AppData [sourceLocation=" + sourceLocation + ", mongoHost=" + mongoHost + ", mongoId=" + mongoId
				+ "]";
	}
}
