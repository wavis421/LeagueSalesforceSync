package model;

public class StaffMemberModel {
	// clientID and staffID are identical except for TA's
	private String clientID, sfClientID, accountID;
	private String firstName, lastName, birthdate;
	private String githubName, homeLocation;
	private String category; // teacher, volunteer, TA
	private String occupation, employer, startInfo, title;
	private String gender, phone, homePhone, address, email, alternateEmail, liveScan, tShirt;
	private String role, whereDidYouHear, leave, emergName, emergEmail, emergPhone;
	private boolean isBoardMember, isStaffMember, isKeyHolder, isAlsoClient;
	private int pastEvents, futureEvents;

	public StaffMemberModel(String clientID, String sfClientID, String firstName, String lastName, String category,
			String role, String occupation, String employer, String startInfo, String gender, String birthdate,
			String phone, String homePhone, String address, String email, String alternateEmail, String homeLocation,
			String githubName, int pastEvents, int futureEvents, boolean keyHolder, String liveScan, String tShirt,
			String whereDidYouHear, String leave, String emergName, String emergEmail, String emergPhone,
			boolean isBoardMember, boolean isStaffMember, boolean isAlsoClient, String title) {

		this.clientID = clientID;
		if (sfClientID != null && (sfClientID.startsWith("null") || sfClientID.equals("")))
			this.sfClientID = null;
		else
			this.sfClientID = sfClientID;
		this.firstName = firstName;
		this.lastName = lastName;
		this.category = category;
		this.role = role;
		this.occupation = occupation;
		this.employer = employer;
		this.startInfo = startInfo;
		this.gender = gender;
		this.birthdate = birthdate;
		this.phone = phone;
		this.homePhone = homePhone;
		this.address = address;
		this.email = email;
		this.alternateEmail = alternateEmail;
		this.homeLocation = homeLocation;
		this.githubName = githubName;
		this.pastEvents = pastEvents;
		this.futureEvents = futureEvents;
		this.isKeyHolder = keyHolder;
		this.liveScan = liveScan;
		this.tShirt = tShirt;
		this.whereDidYouHear = whereDidYouHear;
		this.leave = leave;
		this.emergName = emergName;
		this.emergEmail = emergEmail;
		this.emergPhone = emergPhone;
		this.isBoardMember = isBoardMember;
		this.isStaffMember = isStaffMember;
		this.isAlsoClient = isAlsoClient;
		this.title = title;
	}

	@Override
	public String toString() {
		return clientID + ", " + sfClientID + ": " + firstName + " " + lastName + " (" + category + ")";
	}

	public String getClientID() {
		return clientID;
	}

	public String getSfClientID() {
		if (sfClientID == null)
			return clientID;
		else
			return sfClientID;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public String getFullName() {
		return firstName + " " + lastName;
	}

	public String getCategory() {
		return category;
	}

	public String getOccupation() {
		return occupation;
	}

	public String getEmployer() {
		return employer;
	}

	public String getStartInfo() {
		return startInfo;
	}

	public String getGender() {
		return gender;
	}

	public String getHomePhone() {
		return homePhone;
	}

	public String getAlternateEmail() {
		return alternateEmail;
	}

	public boolean getKeyHolder() {
		return isKeyHolder;
	}

	public String getLiveScan() {
		return liveScan;
	}

	public String getTShirt() {
		return tShirt;
	}

	public String getWhereDidYouHear() {
		return whereDidYouHear;
	}

	public String getLeave() {
		return leave;
	}

	public String getEmergName() {
		return emergName;
	}

	public String getEmergEmail() {
		return emergEmail;
	}

	public String getEmergPhone() {
		return emergPhone;
	}

	public boolean isBoardMember() {
		return isBoardMember;
	}

	public boolean isStaffMember() {
		return isStaffMember;
	}

	public String getBirthdate() {
		return birthdate;
	}

	public String getGithubName() {
		return githubName;
	}

	public String getHomeLocation() {
		return homeLocation;
	}

	public String getPhone() {
		return phone;
	}

	public String getAddress() {
		return address;
	}

	public String getEmail() {
		return email;
	}

	public String gettShirt() {
		return tShirt;
	}

	public String getRole() {
		return role;
	}

	public boolean isKeyHolder() {
		return isKeyHolder;
	}

	public int getPastEvents() {
		return pastEvents;
	}

	public int getFutureEvents() {
		return futureEvents;
	}

	public boolean isAlsoClient() {
		return isAlsoClient;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String id) {
		accountID = id;
	}

	public String getTitle() {
		return title;
	}
}
