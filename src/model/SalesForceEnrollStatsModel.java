package model;

// Model for importing Enrollment Stats from Pike13 into SalesForce
public class SalesForceEnrollStatsModel implements Comparable<SalesForceEnrollStatsModel> {
	private int clientID;
	private int enrollStats;

	public SalesForceEnrollStatsModel(int clientID, int enrollStats) {
		this.clientID = clientID;
		this.enrollStats = enrollStats;
	}

	public int getClientID() {
		return clientID;
	}

	public int getEnrollStats() {
		return enrollStats;
	}

	@Override
	public int compareTo(SalesForceEnrollStatsModel other) {
		if (this.clientID < other.getClientID())
			return -1;
		else if (this.clientID > other.getClientID())
			return 1;
		else
			return 0;
	}
}
