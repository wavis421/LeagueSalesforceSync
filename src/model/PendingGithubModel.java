package model;

/**
 * PendingGithubModel: This class contains github commit data for all League github classrooms.
 *        The data is populated using a Github "hook", and then removed after processing.
 *        
 * @author wavis
 *
 */
public class PendingGithubModel {
	private int primaryID;
	private String gitUser;
	private String repoName;
	private String serviceDate;
	private String comments;

	public PendingGithubModel(int primaryID, String gitUser, String repoName, String serviceDate, String comments) {
		this.primaryID = primaryID;
		this.gitUser = gitUser;
		this.repoName = repoName;
		this.serviceDate = serviceDate;
		this.comments = comments;
	}

	public int getPrimaryID() {
		return primaryID;
	}

	public String getGitUser() {
		return gitUser;
	}

	public String getRepoName() {
		return repoName;
	}

	public String getServiceDate() {
		return serviceDate;
	}

	public String getComments() {
		return comments;
	}
}
