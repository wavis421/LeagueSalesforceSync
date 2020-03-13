package model;

public class StudentImportModel implements Comparable<StudentImportModel> {
	private int     clientID;
	private String  lastName, firstName, githubName, startDate, homeLocString, currLevel;
	private int     homeLocation, gender, gradYear, isInMasterDb;

	// Additional fields for importing contacts to SalesForce
	private String  gradYearString;
	private String  genderString;
	private String  birthDate = "";
	private String  currGrade, lastExamScore = "", currentClass, registerClass;
	private String  email, phone1, phone2, address, schoolName, financialAidPercent, grantInfo;
	private String  membership, passOnFile, leaveReason, hearAboutUs, whoToThank;
	private String  emergContactName, emergContactPhone, emergContactEmail;
	private String  accountID, accountMgrNames, accountMgrPhones, accountMgrEmails, dependentNames;
	private String  staffSinceDate = "";
	private int     completedVisits, futureVisits, staffPastEvents;
	private boolean signedWaiver, stopEmail, financialAid;
	private Object  sfContact;
	
	public String   relationToStudent, phone1_type, phone2_type, prefContactMethod, primaryLanguage;
	public String   prefContactLang, emergRelationToStud, prefClassLoc, studAddrIfDiff;
	public String   schoolLevel1, schoolLevel2, studEthnicity, studRace,techAccess, questions;
	public boolean  studAddrSame, workComputers;

	public StudentImportModel(int clientID, String lastName, String firstName, String githubName, String gender,
			String startDate, String homeLocation, String gradYear, String email, String acctMgrEmail,
			String emergEmail, String phone_1, String acctMgrPhones, String phone_2, String emergContactPhone,
			String birthDate, String currentLevel, int futureVisits, String lastExamScore) {

		// Pike13 import data
		this.clientID = clientID;
		this.lastName = lastName;
		this.firstName = firstName;
		this.githubName = parseGithubName(githubName);
		this.gender = GenderModel.convertStringToGender(gender);
		this.startDate = startDate;
		this.birthDate = birthDate;
		this.currLevel = currentLevel;
		this.futureVisits = futureVisits;
		if (lastExamScore != null)
			this.lastExamScore = lastExamScore;

		this.homeLocation = LocationLookup.convertStringToLocation(homeLocation);
		this.homeLocString = LocationLookup.convertLocationToString(this.homeLocation);

		if (gradYear.equals("") || gradYear.equals("\"\""))
			this.gradYear = 0;
		else {
			try {
				this.gradYear = Integer.parseInt(gradYear);

			} catch (NumberFormatException e) {
				this.gradYear = 0;
			}
		}

		if (email != null)
			this.email = email.trim().replaceAll("[\t\n\r]", "");
		if (acctMgrEmail != null)
			this.accountMgrEmails = acctMgrEmail.trim().replaceAll("[\t\n\r]", "");
		if (emergEmail != null)
			this.emergContactEmail = emergEmail.trim().replaceAll("[\t\n\r]", "");

		if (phone_1 != null)
			this.phone1 = parsePhone(phone_1);
		if (acctMgrPhones != null)
			this.accountMgrPhones = parsePhone(acctMgrPhones);
		if (phone_2 != null)
			this.phone2 = parsePhone(phone_2);
		if (emergContactPhone != null)
			this.emergContactPhone = parsePhone(emergContactPhone);

		isInMasterDb = 1;
	}

	public StudentImportModel(int clientID, String lastName, String firstName, String githubName, int gender,
			String startDate, int homeLocation, int gradYear, int isInMasterDb, String email, String acctMgrEmail,
			String emergEmail, String phone_1, String acctMgrPhones, String phone_2, String emergContactPhone,
			String birthdate, String staffSinceDate, int staffPastEvents, String currentLevel, String currentClass,
			String lastScore, String registerClass) {

		// Database format being converted for comparison purposes
		this.clientID = clientID;
		this.lastName = lastName;
		this.firstName = firstName;
		this.githubName = parseGithubName(githubName);
		this.gender = gender;
		this.startDate = startDate;
		this.homeLocation = homeLocation;
		this.gradYear = gradYear;
		this.isInMasterDb = isInMasterDb;
		this.birthDate = birthdate;
		this.staffSinceDate = staffSinceDate;
		this.staffPastEvents = staffPastEvents;
		this.currLevel = currentLevel;
		this.currentClass = currentClass;
		this.lastExamScore = lastScore;
		this.registerClass = registerClass;

		this.email = email;
		this.accountMgrEmails = acctMgrEmail;
		this.emergContactEmail = emergEmail;
		this.phone1 = phone_1;
		this.accountMgrPhones = acctMgrPhones;
		this.phone2 = phone_2;
		this.emergContactPhone = emergContactPhone;
	}

	public StudentImportModel(int clientID, String firstName, String lastName, String gender, String birthDate,
			String currGrade, String gradYear, String startDate, String homeLocation, String email, String phone_1,
			String address, String schoolName, String githubName, int completedVisits, int futureVisits,
			boolean signedWaiver, String membership, String passOnFile, boolean stopEmail,
			boolean financialAid, String financialAidPercent, String grantInfo, String leaveReason, String hearAboutUs,
			String whoToThank, String emergContactName, String emergContactPhone, String emergContactEmail,
			String phone_2, String accountMgrNames, String accountMgrPhones, String accountMgrEmails,
			String dependentNames, String currentLevel) {

		this.clientID = clientID;
		this.firstName = firstName;
		this.lastName = lastName;
		this.genderString = gender;
		this.birthDate = birthDate;
		this.currGrade = currGrade;
		this.gradYearString = gradYear;
		this.startDate = startDate;
		this.homeLocString = homeLocation;
		this.email = email;
		this.phone1 = phone_1;
		this.address = address;
		this.schoolName = schoolName;
		this.githubName = parseGithubName(githubName);
		this.completedVisits = completedVisits;
		this.futureVisits = futureVisits;
		this.signedWaiver = signedWaiver;
		this.membership = membership;
		this.passOnFile = passOnFile;
		this.stopEmail = stopEmail;
		this.financialAid = financialAid;
		this.financialAidPercent = financialAidPercent;
		this.grantInfo = grantInfo;
		this.leaveReason = leaveReason;
		this.hearAboutUs = hearAboutUs;
		this.whoToThank = whoToThank;
		this.emergContactName = emergContactName;
		this.emergContactPhone = emergContactPhone;
		this.emergContactEmail = emergContactEmail;
		this.accountMgrNames = accountMgrNames;
		this.accountMgrPhones = accountMgrPhones;
		this.accountMgrEmails = accountMgrEmails;
		this.phone2 = phone_2;
		this.currLevel = currentLevel;
		if (dependentNames == null)
			this.dependentNames = "";
		else
			this.dependentNames = dependentNames;
	}
	
	public void addMoreFields (String relation, String phn1_type, String phn2_type, String contactMethod,
			String primLang, String contactLang, String emergRelation, String prefLoc, boolean addrSame, String studAddr, boolean workComp,
			String level1, String level2, String ethnicity, String race, String tech, String questions)
	{
		this.relationToStudent = relation;
		this.phone1_type = phn1_type;
		this.phone2_type = phn2_type;
		this.prefContactMethod = contactMethod;
		this.primaryLanguage = primLang;
		this.prefContactLang = contactLang;
		this.emergRelationToStud = emergRelation;
		this.prefClassLoc = prefLoc;
		this.studAddrSame = addrSame;
		this.studAddrIfDiff = studAddr;
		this.workComputers = workComp;
		this.schoolLevel1 = level1;
		this.schoolLevel2 = level2;
		this.studEthnicity = ethnicity;
		this.studRace = race;
		this.techAccess = tech;
		this.questions = questions;
	}
	
	private String parseGithubName(String githubName) {
		if (githubName == null || githubName.equals("") || githubName.equals("\"\""))
			githubName = "";
		else {
			int index = githubName.indexOf('(');
			if (index != -1)
				githubName = githubName.substring(0, index);
			githubName.trim();
		}
		return githubName;
	}

	public String toString() {
		return firstName + " " + lastName + " (" + clientID + ")";
	}

	public int getClientID() {
		return clientID;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String id) {
		accountID = id;
	}

	public String getLastName() {
		return lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getFullName() {
		return firstName + " " + lastName;
	}

	public String getGithubName() {
		return githubName;
	}

	public int getGender() {
		return gender;
	}

	public String getStartDate() {
		return startDate;
	}

	public String getHomeLocAsString() {
		return homeLocString;
	}

	public int getHomeLocation() {
		return homeLocation;
	}

	public int getGradYear() {
		return gradYear;
	}

	public int getIsInMasterDb() {
		return isInMasterDb;
	}

	public String getHomeLocString() {
		return homeLocString;
	}

	public String getGradYearString() {
		return gradYearString;
	}

	public String getGenderString() {
		return genderString;
	}

	public String getBirthDate() {
		return birthDate;
	}

	public String getCurrGrade() {
		return currGrade;
	}

	public String getCurrLevel() {
		return currLevel;
	}

	public void setCurrLevel(String level) {
		currLevel = level;
	}
	
	public String getCurrClass() {
		return currentClass;
	}

	public String getRegClass() {
		return registerClass;
	}

	public String getLastExamScore() {
		return lastExamScore;
	}

	public String getEmail() {
		return email;
	}

	public String getPhone1() {
		return phone1;
	}

	public String getAddress() {
		return address;
	}

	public String getSchoolName() {
		return schoolName;
	}

	public String getFinancialAidPercent() {
		return financialAidPercent;
	}

	public String getGrantInfo() {
		return grantInfo;
	}

	public String getLeaveReason() {
		if (leaveReason.equals(""))
			return " ";
		else
			return leaveReason;
	}

	public String getHearAboutUs() {
		return hearAboutUs;
	}

	public String getWhoToThank() {
		return whoToThank;
	}

	public String getEmergContactName() {
		return emergContactName;
	}

	public String getEmergContactPhone() {
		return emergContactPhone;
	}

	public String getEmergContactEmail() {
		return emergContactEmail;
	}

	public int getCompletedVisits() {
		return completedVisits;
	}

	public int getFutureVisits() {
		return futureVisits;
	}

	public boolean isSignedWaiver() {
		return signedWaiver;
	}

	public String getMembership() {
		return membership;
	}

	public String getPassOnFile() {
		return passOnFile;
	}

	public boolean isStopEmail() {
		return stopEmail;
	}

	public boolean isFinancialAid() {
		return financialAid;
	}

	public String getPhone2() {
		return phone2;
	}

	public String getAccountMgrNames() {
		return accountMgrNames;
	}

	public String getAccountMgrPhones() {
		return accountMgrPhones;
	}

	public String getAccountMgrEmails() {
		return accountMgrEmails;
	}

	public String getDependentNames() {
		return dependentNames;
	}

	public Object getSfContact() {
		return sfContact;
	}

	public String getStaffSinceDate() {
		return staffSinceDate;
	}

	public int getStaffPastEvents() {
		return staffPastEvents;
	}

	public void setStaffData(String staffSinceDate, int staffPastEvents) {
		// Set data for students who are TA's
		this.staffSinceDate = staffSinceDate;
		this.staffPastEvents = staffPastEvents;
	}

	public void setSfContact(Object sfContact) {
		this.sfContact = sfContact;
	}

	@Override
	public int compareTo(StudentImportModel other) {
		if (clientID < other.getClientID())
			return -1;

		else if (clientID > other.getClientID())
			return 1;

		// Client ID matches
		else if (lastName.equals(other.getLastName()) && firstName.equals(other.getFirstName())
				&& githubName.equals(other.getGithubName()) && startDate.equals(other.getStartDate())
				&& homeLocation == other.getHomeLocation() && gender == other.getGender()
				&& gradYear == other.getGradYear() && isInMasterDb == other.getIsInMasterDb()
				&& email.equals(other.getEmail()) && emergContactEmail.equals(other.getEmergContactEmail())
				&& accountMgrEmails.equals(other.getAccountMgrEmails()) && phone1.equals(other.getPhone1())
				&& accountMgrPhones.equals(other.getAccountMgrPhones()) && phone2.equals(other.getPhone2())
				&& emergContactPhone.equals(other.getEmergContactPhone()) && birthDate.equals(other.getBirthDate())
				&& staffSinceDate.equals(other.getStaffSinceDate()) && staffPastEvents == other.getStaffPastEvents()
				&& currLevel.equals(other.currLevel) && lastExamScore.equals(other.lastExamScore)) {
			return 0;
		}

		else {
			// Client ID matches but data does not
			return 2;
		}
	}

	public String displayAll() {
		return (clientID + ": " + firstName + " " + lastName + " (" + gender + "), github: " + githubName + ", home: "
				+ homeLocString + ", start: " + startDate + ", grad: " + gradYear + ", " + isInMasterDb);
	}

	private static String parsePhone(String origPhone) {
		String phoneList = origPhone.trim();
		if (phoneList.length() < 10)
			return origPhone;

		String phone = "", resultPhone = "";
		String[] values = phoneList.split("\\s*,\\s*");

		for (int i = 0; i < values.length; i++) {
			phone = values[i].trim();

			if (phone.length() == 10 && phone.indexOf('(') == -1 && phone.indexOf('-') == -1)
				phone = "(" + phone.substring(0, 3) + ") " + phone.substring(3, 6) + "-" + phone.substring(6);
			else if (phone.length() == 12
					&& (phone.charAt(3) == '-' || phone.charAt(3) == '.' || phone.charAt(3) == ' ')
					&& (phone.charAt(7) == '-' || phone.charAt(7) == '.' || phone.charAt(7) == ' '))
				phone = "(" + phone.substring(0, 3) + ") " + phone.substring(4, 7) + "-" + phone.substring(8);
			else if (phone.length() == 13 && phone.charAt(0) == '(' && phone.charAt(4) == ')' && phone.charAt(8) == '-')
				phone = phone.substring(0, 5) + " " + phone.substring(5);
			else if (phone.length() == 11 && phone.charAt(3) == ' ')
				phone = "(" + phone.substring(0, 3) + ") " + phone.substring(4, 7) + "-" + phone.substring(7);
			else if (phone.length() == 11 && phone.charAt(0) == '1')
				phone = "(" + phone.substring(1, 4) + ") " + phone.substring(4, 7) + "-" + phone.substring(7);

			if (!resultPhone.equals(""))
				resultPhone += ", ";
			resultPhone += phone;
		}

		return resultPhone;
	}
}
