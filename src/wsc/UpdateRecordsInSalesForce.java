package wsc;

import java.util.ArrayList;
import java.util.Calendar;

import org.joda.time.DateTime;

import com.sforce.soap.enterprise.Address;
import com.sforce.soap.enterprise.DeleteResult;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.Error;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.SaveResult;
import com.sforce.soap.enterprise.UpsertResult;
import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.soap.enterprise.sobject.Staff_Hours__c;
import com.sforce.soap.enterprise.sobject.Student_Attendance__c;
import com.sforce.ws.ConnectionException;

import model.AttendanceEventModel;
import model.LogDataModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.SalesForceStaffHoursModel;
import model.StaffMemberModel;
import model.StudentImportModel;
import model.StudentNameModel;

public class UpdateRecordsInSalesForce {
	private static final int MAX_NUM_UPSERT_RECORDS = 200;

	private MySqlDatabase sqlDb;
	private EnterpriseConnection connection;
	private GetRecordsFromSalesForce getRecords;

	private int clientUpdateCount = 0;
	private int attendanceUpsertCount = 0;
	private int staffHoursUpsertCount = 0;

	public UpdateRecordsInSalesForce(MySqlDatabase sqlDb, EnterpriseConnection connection,
			GetRecordsFromSalesForce getRecords) {
		this.sqlDb = sqlDb;
		this.connection = connection;
		this.getRecords = getRecords;
	}

	public void updateStudents(ArrayList<StudentImportModel> pike13Students,
			ArrayList<StudentImportModel> pike13Managers, ArrayList<Account> sfAccounts) {
		ArrayList<Contact> recordList = new ArrayList<Contact>();
		clientUpdateCount = 0;

		try {
			for (int i = 0; i < pike13Students.size(); i++) {
				// Add each Pike13 client record to SalesForce list
				StudentImportModel student = pike13Students.get(i);

				// Ignore school clients!
				if (student.getFullName().equals("gompers Prep") || student.getFullName().equals("wilson Middle School")
						|| student.getFullName().equals("nativity Prep Academy")
						|| student.getFullName().equals("Sample Customer")
						|| student.getFullName().equals("barrio Logan College Institute")
						|| student.getFirstName().startsWith("HOLIDAY"))
					continue;

				// Check for account manager names
				if (student.getAccountMgrNames() == null || student.getAccountMgrNames().equals("")) {
					// Student has no Pike13 account manager, so error
					sqlDb.insertLogData(LogDataModel.MISSING_PIKE13_ACCT_MGR_FOR_CLIENT,
							new StudentNameModel(student.getFirstName(), student.getLastName(), true),
							student.getClientID(), " " + student.getFullName());
					continue;
				}

				// Find account manager in Pike13 list so we can parse first/last names
				String accountMgrName = ListUtilities.getFirstNameInString(student.getAccountMgrNames());
				StudentImportModel acctMgrModel = ListUtilities.findAcctManagerInList(student, accountMgrName,
						pike13Managers);
				if (acctMgrModel == null) {
					// Pike13 account manager not found? This should not happen!
					sqlDb.insertLogData(LogDataModel.MISSING_PIKE13_ACCT_MGR_FOR_CLIENT,
							new StudentNameModel(student.getFirstName(), student.getLastName(), true),
							student.getClientID(), " " + student.getFullName() + ", manager " + accountMgrName);
					continue;
				}

				// Find account in SalesForce
				String acctFamilyName = acctMgrModel.getLastName() + " " + acctMgrModel.getFirstName() + " Family";
				Account account = ListUtilities.findAccountInSalesForceList(acctFamilyName, acctMgrModel, sfAccounts);

				if (account.getName().equals("")) {
					// SalesForce account does not yet exist, so create
					account.setName(acctFamilyName);
					account.setType("Family");
					if (!createAccountRecord(student.getFirstName(), student.getLastName(), student.getClientID(), account))
						continue;

					// Now that account has been created, need to get account from SF again
					account = getRecords.getSalesForceAccountByName(acctFamilyName);
					if (account == null || account.getName().equals(""))
						continue;

					// Add to list in case more dependents belong to this account
					sfAccounts.add(account);
				}

				// Create contact and add to list
				recordList.add(createContactRecord(false, student, account));
			}

			upsertContactRecordList(recordList, "student");

		} catch (Exception e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + clientUpdateCount + " student records processed");
	}

	public void updateAdults(ArrayList<StudentImportModel> pike13Students, ArrayList<StudentImportModel> pike13Adults,
			ArrayList<Account> sfAccounts) {
		ArrayList<Contact> recordList = new ArrayList<Contact>();
		clientUpdateCount = 0;

		try {
			for (int i = 0; i < pike13Adults.size(); i++) {
				// Add each Pike13 client record to SalesForce list
				StudentImportModel adult = pike13Adults.get(i);

				// Get 1st dependent name, use this to get SalesForce Account
				String dependentName = ListUtilities.getFirstNameInString(adult.getDependentNames());
				Account account = ListUtilities.findAccountNameInList(dependentName, pike13Students, pike13Adults,
						sfAccounts);

				if (account.getName().equals(""))
					// Dependent is either hidden/deleted or this is test account
					continue;

				// Create contact and add to list
				recordList.add(createContactRecord(true, adult, account));
			}

			upsertContactRecordList(recordList, "adult");

		} catch (Exception e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + clientUpdateCount + " adult records processed");
	}

	public void updateAttendance(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			ArrayList<AttendanceEventModel> dbAttendance, ArrayList<Contact> contacts) {
		ArrayList<Student_Attendance__c> recordList = new ArrayList<Student_Attendance__c>();

		try {
			for (int i = 0; i < pike13Attendance.size(); i++) {
				// Add each Pike13 attendance record to SalesForce list
				SalesForceAttendanceModel inputModel = pike13Attendance.get(i);

				Contact c = ListUtilities.findClientIDInList(LogDataModel.MISSING_SF_CONTACT_FOR_ATTENDANCE,
						inputModel.getClientID(), null, contacts);
				if (c == null)
					continue;

				Student_Attendance__c a = new Student_Attendance__c();
				a.setContact__r(c);
				a.setEvent_Name__c(inputModel.getEventName());
				a.setService_Name__c(inputModel.getServiceName());
				a.setEvent_Type__c(inputModel.getEventType());
				a.setStatus__c(inputModel.getStatus());
				a.setSchedule_id__c(inputModel.getScheduleID());
				a.setVisit_Id__c(inputModel.getVisitID());
				a.setLocation__c(inputModel.getLocation());
				a.setStaff__c(inputModel.getStaff());
				AttendanceEventModel attend = ListUtilities.findAttendanceEventInList(inputModel.getVisitID(),
						dbAttendance);
				if (attend != null) {
					if (!attend.getGithubComments().equals(""))
						a.setNote__c(attend.getGithubComments());
					if (attend.getRepoName() != null && !attend.getRepoName().equals(""))
						a.setRepo_Name__c(attend.getRepoName());
				}

				a.setService_Date__c(convertDateStringToCalendar(inputModel.getServiceDate()));
				a.setService_TIme__c(inputModel.getServiceTime());

				recordList.add(a);
			}

			// Copy up to 200 records to array at a time (max allowed)
			Student_Attendance__c[] recordArray;
			int numRecords = recordList.size();
			int remainingRecords = numRecords;
			int arrayIdx = 0;

			System.out.println(numRecords + " Pike13 attendance records");
			if (numRecords > MAX_NUM_UPSERT_RECORDS)
				recordArray = new Student_Attendance__c[MAX_NUM_UPSERT_RECORDS];
			else
				recordArray = new Student_Attendance__c[numRecords];

			for (int i = 0; i < numRecords; i++) {
				recordArray[arrayIdx] = recordList.get(i);

				arrayIdx++;
				if (arrayIdx == MAX_NUM_UPSERT_RECORDS) {
					upsertAttendanceRecords(recordArray);
					remainingRecords -= MAX_NUM_UPSERT_RECORDS;
					arrayIdx = 0;

					if (remainingRecords > MAX_NUM_UPSERT_RECORDS)
						recordArray = new Student_Attendance__c[MAX_NUM_UPSERT_RECORDS];
					else if (remainingRecords > 0)
						recordArray = new Student_Attendance__c[remainingRecords];
				}
			}

			// Update remaining records in Salesforce.com
			if (arrayIdx > 0)
				upsertAttendanceRecords(recordArray);

		} catch (Exception e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SF_ATTENDANCE_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						"");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_ATTENDANCE_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_ATTENDANCE_UPDATED, new StudentNameModel("", "", false), 0,
				", " + attendanceUpsertCount + " records processed");
	}

	public void removeExtraAttendanceRecords(ArrayList<SalesForceAttendanceModel> pike13Attendance, String startDate,
			String endDate) {
		String serviceDate;
		boolean done = false;
		QueryResult queryResult;
		ArrayList<String> deleteList = new ArrayList<String>();

		try {
			queryResult = connection
					.query("SELECT Id, Visit_Id__c, Service_Date__c, Front_Desk_ID__c, Event_Name__c, Owner.Name "
							+ "FROM Student_Attendance__c WHERE Owner.Name = 'League Bot' AND Visit_Id__c != NULL "
							+ "AND Service_Date__c >= " + startDate + " AND Service_Date__c <= " + endDate
							+ " ORDER BY Visit_Id__c ASC");
			System.out.println(queryResult.getSize() + " Salesforce attendance records for League Bot");

			if (queryResult.getSize() > 0) {
				while (!done) {
					SObject[] records = queryResult.getRecords();
					for (int i = 0; i < records.length; i++) {
						// Check whether attendance record exists in Pike13
						Student_Attendance__c a = (Student_Attendance__c) records[i];
						if (!ListUtilities.findVisitIdInList(a.getVisit_Id__c(), pike13Attendance)) {
							// Record not found, so add to deletion list
							serviceDate = (new DateTime(a.getService_Date__c().getTimeInMillis()))
									.toString("yyyy-MM-dd");
							deleteList.add(a.getId());

							sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_RECORD,
									new StudentNameModel("", "", false), Integer.parseInt(a.getFront_Desk_ID__c()),
									" " + a.getVisit_Id__c() + " " + a.getEvent_Name__c() + " on " + serviceDate
											+ " for ClientID " + a.getFront_Desk_ID__c());
						}
					}
					if (queryResult.isDone())
						done = true;
					else
						queryResult = connection.queryMore(queryResult.getQueryLocator());
				}

				// Delete obsolete attendance records
				if (deleteList.size() > 0)
					deleteAttendanceRecords((String[]) deleteList.toArray(new String[0]));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_CANCELED_ATTEND_CLEANUP, new StudentNameModel("", "", false), 0,
				", " + deleteList.size() + " records deleted");
	}

	public void updateStaffMembers(ArrayList<StaffMemberModel> pike13StaffMembers, ArrayList<Contact> sfContacts,
			ArrayList<Account> sfAccounts) {
		ArrayList<Contact> recordList = new ArrayList<Contact>();
		clientUpdateCount = 0;

		try {
			for (int i = 0; i < pike13StaffMembers.size(); i++) {
				// Add each Pike13 staff record to SalesForce list
				StaffMemberModel staff = pike13StaffMembers.get(i);

				String firstName = staff.getFirstName();
				String clientID = staff.getClientID();
				String accountID;

				if (firstName.startsWith("TA-")) {
					// Remove TA- from front of string
					firstName = firstName.substring(3);

					// TA's must have valid SFClientID that is different from ClientID
					if (clientID.equals(staff.getSfClientID())) {
						sqlDb.insertLogData(LogDataModel.MISSING_SF_CLIENT_ID_FOR_TA,
								new StudentNameModel(firstName, staff.getLastName(), false), Integer.parseInt(clientID),
								" for " + staff.getFullName());
						continue;
					}

					// TA's use SF Client ID
					clientID = staff.getSfClientID();
				}

				Contact c = ListUtilities.findClientIDInList(-1, clientID, staff.getFullName(), sfContacts);
				if (c == null) {
					// TODO: Add new contacts
					accountID = getStaffAccountID(staff, sfAccounts);
					if (accountID == null)
						System.out.println("Failed to create staff account: " + staff.getFullName() + ", "
								+ staff.getCategory() + ", is Staff=" + staff.isStaffMember() + ", is Board="
								+ staff.isBoardMember() + staff.getClientID() + ", " + staff.getSfClientID());
					else
						System.out.println("New staff account: " + staff.getFullName() + ", " + staff.getCategory()
								+ ", is Staff=" + staff.isStaffMember() + ", is Board=" + staff.isBoardMember() + ", "
								+ staff.getClientID() + ", " + staff.getSfClientID() + ", " + accountID);
					continue;
				} else
					accountID = c.getAccountId();

				// Add to list
				recordList.add(createStaffRecord(staff, firstName, clientID, accountID));
			}

			upsertContactRecordList(recordList, "staff");

		} catch (Exception e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + clientUpdateCount + " staff records processed");
	}

	public void updateStaffHours(ArrayList<StaffMemberModel> pike13StaffMembers,
			ArrayList<SalesForceStaffHoursModel> pike13StaffHours, ArrayList<Contact> contacts) {
		ArrayList<Staff_Hours__c> recordList = new ArrayList<Staff_Hours__c>();

		System.out.println(pike13StaffHours.size() + " Staff Hour records from Pike13");

		try {
			for (int i = 0; i < pike13StaffHours.size(); i++) {
				// Add each Pike13 staff hours record to SalesForce list
				SalesForceStaffHoursModel inputModel = pike13StaffHours.get(i);

				String staffID = ListUtilities.findStaffIDInList(inputModel.getClientID(), pike13StaffMembers);
				if (staffID == null)
					continue;

				Contact c = ListUtilities.findClientIDInList(LogDataModel.MISSING_SALES_FORCE_STAFF_MEMBER, staffID,
						inputModel.getFullName(), contacts);
				if (c == null)
					continue;

				Staff_Hours__c h = new Staff_Hours__c();
				h.setStaff_Name__r(c);
				h.setEvent_Name__c(inputModel.getEventName());
				h.setService__c(inputModel.getServiceName());
				h.setSchedule_ID__c(inputModel.getScheduleID());
				h.setSchedule_client_ID__c(inputModel.getScheduleID() + inputModel.getClientID());
				h.setLocation_Full__c(inputModel.getLocation());
				h.setHours__c(inputModel.getHours());
				h.setPresent__c(inputModel.getCompleted());
				h.setAbsent__c(inputModel.getNoShow());
				h.setExcused__c(inputModel.getLateCanceled());

				h.setService_Date__c(convertDateStringToCalendar(inputModel.getServiceDate()));
				h.setService_Time__c(inputModel.getServiceTime());

				recordList.add(h);
			}

			// Copy up to 200 records to array at a time (max allowed)
			Staff_Hours__c[] recordArray;
			int numRecords = recordList.size();
			int remainingRecords = numRecords;
			int arrayIdx = 0;

			if (numRecords > MAX_NUM_UPSERT_RECORDS)
				recordArray = new Staff_Hours__c[MAX_NUM_UPSERT_RECORDS];
			else
				recordArray = new Staff_Hours__c[numRecords];

			for (int i = 0; i < numRecords; i++) {
				recordArray[arrayIdx] = recordList.get(i);

				arrayIdx++;
				if (arrayIdx == MAX_NUM_UPSERT_RECORDS) {
					upsertStaffHoursRecords(recordArray);
					remainingRecords -= MAX_NUM_UPSERT_RECORDS;
					arrayIdx = 0;

					if (remainingRecords > MAX_NUM_UPSERT_RECORDS)
						recordArray = new Staff_Hours__c[MAX_NUM_UPSERT_RECORDS];
					else if (remainingRecords > 0)
						recordArray = new Staff_Hours__c[remainingRecords];
				}
			}

			// Update remaining records in Salesforce.com
			if (arrayIdx > 0)
				upsertStaffHoursRecords(recordArray);

		} catch (Exception e) {
			e.printStackTrace();
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_STAFF_HOURS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + staffHoursUpsertCount + " records processed");
	}

	private void upsertClientRecords(Contact[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Front_Desk_ID__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR,
							new StudentNameModel("", "", false), 0, ": " + errors[j].getMessage());
				}
			} else
				clientUpdateCount++;
		}
	}

	private void upsertAttendanceRecords(Student_Attendance__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Visit_Id__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR,
							new StudentNameModel("", "", false), 0, ": " + errors[j].getMessage());
				}
			} else
				attendanceUpsertCount++;
		}
	}

	private void deleteAttendanceRecords(String[] records) {
		DeleteResult[] deleteResults = null;

		try {
			// Update the records in Salesforce.com
			deleteResults = connection.delete(records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < deleteResults.length; i++) {
			if (!deleteResults[i].isSuccess()) {
				Error[] errors = deleteResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_ERROR,
							new StudentNameModel("", "", false), 0, ": " + errors[j].getMessage());
				}
			}
		}
	}

	private void upsertStaffHoursRecords(Staff_Hours__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("schedule_client_ID__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR,
							new StudentNameModel("", "", false), 0,
							" (" + records[i].getSchedule_client_ID__c() + "): " + errors[j].getMessage());
				}
			} else
				staffHoursUpsertCount++;
		}
	}

	private void upsertContactRecordList(ArrayList<Contact> recordList, String recordType) {
		// Copy up to 200 records to array at a time (max allowed)
		Contact[] recordArray;
		int numRecords = recordList.size();
		int remainingRecords = numRecords;
		int arrayIdx = 0;

		System.out.println(numRecords + " Pike13 " + recordType + " records");
		if (numRecords > MAX_NUM_UPSERT_RECORDS)
			recordArray = new Contact[MAX_NUM_UPSERT_RECORDS];
		else
			recordArray = new Contact[numRecords];

		for (int i = 0; i < numRecords; i++) {
			recordArray[arrayIdx] = recordList.get(i);

			arrayIdx++;
			if (arrayIdx == MAX_NUM_UPSERT_RECORDS) {
				upsertClientRecords(recordArray);
				remainingRecords -= MAX_NUM_UPSERT_RECORDS;
				arrayIdx = 0;

				if (remainingRecords > MAX_NUM_UPSERT_RECORDS)
					recordArray = new Contact[MAX_NUM_UPSERT_RECORDS];
				else if (remainingRecords > 0)
					recordArray = new Contact[remainingRecords];
			}
		}

		// Update remaining records in Salesforce.com
		if (arrayIdx > 0)
			upsertClientRecords(recordArray);
	}

	private Contact createContactRecord(boolean adult, StudentImportModel contact, Account account) {
		// Create contact and add fields
		Contact c = new Contact();

		c.setAccountId(account.getId());
		c.setFront_Desk_Id__c(String.valueOf(contact.getClientID()));
		c.setFirstName(contact.getFirstName());
		c.setLastName(contact.getLastName());
		if (adult)
			c.setContact_Type__c("Adult");
		else
			c.setContact_Type__c("Student");
		if (contact.getEmail() != null)
			c.setEmail(contact.getEmail());
		if (contact.getMobilePhone() != null)
			c.setMobilePhone(parsePhone(contact.getMobilePhone()));
		if (contact.getHomePhone() != null)
			c.setHomePhone(parsePhone(contact.getHomePhone()));
		if (contact.getAddress() != null) {
			c.setFull_Address__c(contact.getAddress());
			Address addr = parseAddress(contact.getAddress());
			if (addr != null) {
				c.setMailingStreet(addr.getStreet());
				c.setMailingCity(addr.getCity());
				c.setMailingState("California");
				c.setMailingCountry("United States");
				c.setMailingPostalCode(addr.getPostalCode());
			}
		}
		c.setPast_Events__c((double) contact.getCompletedVisits());
		c.setFuture_Events__c((double) contact.getFutureVisits());
		c.setSigned_Waiver__c(contact.isSignedWaiver());
		c.setMembership__c(contact.getMembership());
		if (contact.getPassOnFile() != null)
			c.setPlan__c(contact.getPassOnFile());
		if (contact.getHomeLocAsString() != null)
			c.setHome_Location_Long__c(contact.getHomeLocAsString());
		if (contact.getSchoolName() != null)
			c.setCurrent_School__c(contact.getSchoolName());
		if (contact.gettShirtSize() != null)
			c.setShirt_Size__c(contact.gettShirtSize());
		if (contact.getGenderString() != null)
			c.setGender__c(contact.getGenderString());
		if (contact.getEmergContactName() != null)
			c.setEmergency_Name__c(contact.getEmergContactName());
		if (contact.getEmergContactPhone() != null)
			c.setEmergency_Phone__c(parsePhone(contact.getEmergContactPhone()));
		if (contact.getEmergContactEmail() != null)
			c.setEmergency_Email__c(contact.getEmergContactEmail());
		if (contact.getCurrGrade() != null)
			c.setGrade__c(contact.getCurrGrade());
		if (contact.getHearAboutUs() != null)
			c.setHow_you_heard_about_us__c(contact.getHearAboutUs());
		if (contact.getGradYearString() != null)
			c.setGrad_Year__c(contact.getGradYearString());
		if (contact.getWhoToThank() != null)
			c.setWho_can_we_thank__c(contact.getWhoToThank());
		if (contact.getGithubName() != null)
			c.setGIT_HUB_acct_name__c(contact.getGithubName());
		if (contact.getGrantInfo() != null)
			c.setGrant_Information__c(contact.getGrantInfo());
		c.setLeave_Reason__c(contact.getLeaveReason());
		c.setStop_Email__c(contact.isStopEmail());
		c.setScholarship__c(contact.isFinancialAid());
		if (contact.getFinancialAidPercent() != null)
			c.setScholarship_Percentage__c(contact.getFinancialAidPercent());
		if (contact.getBirthDate() != null && !contact.getBirthDate().equals(""))
			c.setDate_of_Birth__c(convertDateStringToCalendar(contact.getBirthDate()));

		return c;
	}

	private Contact createStaffRecord(StaffMemberModel staff, String firstName, String clientID, String accountID) {
		// Create contact and add fields
		Contact c = new Contact();

		c.setAccountId(accountID);
		c.setFront_Desk_Id__c(clientID);
		c.setFirstName(firstName);
		c.setLastName(staff.getLastName());
		if (staff.getEmail() != null)
			c.setEmail(staff.getEmail());
		if (staff.getAlternateEmail() != null)
			c.setAlternate_Email__c(staff.getAlternateEmail());
		if (staff.getAddress() != null) {
			c.setFull_Address__c(staff.getAddress());
			Address addr = parseAddress(staff.getAddress());
			if (addr != null) {
				c.setMailingStreet(addr.getStreet());
				c.setMailingCity(addr.getCity());
				c.setMailingState("California");
				c.setMailingCountry("United States");
				c.setMailingPostalCode(addr.getPostalCode());
			}
		}
		if (staff.getPhone() != null)
			c.setPhone(parsePhone(staff.getPhone()));
		if (staff.getHomePhone() != null)
			c.setHomePhone(parsePhone(staff.getHomePhone()));
		c.setBoard_Member_Active__c(staff.isBoardMember() ? "Active" : "");
		c.setStaff__c(staff.isStaffMember() ? "Active" : "");
		if (staff.getCategory() != null)
			c.setStaff_Category__c(staff.getCategory());
		if (staff.getRole() != null)
			c.setRole__c(staff.getRole());
		if (staff.getEmergEmail() != null)
			c.setEmergency_Email__c(staff.getEmergEmail());
		if (staff.getEmergName() != null)
			c.setEmergency_Name__c(staff.getEmergName());
		if (staff.getEmergPhone() != null)
			c.setEmergency_Phone__c(parsePhone(staff.getEmergPhone()));
		if (staff.getEmployer() != null)
			c.setEmployer__c(staff.getEmployer());
		if (staff.getGender() != null)
			c.setGender__c(staff.getGender());
		if (staff.getGithubName() != null)
			c.setGIT_HUB_acct_name__c(staff.getGithubName());
		if (staff.getHomeLocation() != null)
			c.setHome_Location_Long__c(staff.getHomeLocation());
		c.setPast_Staff_Events__c((double) staff.getPastEvents());
		c.setFuture_Staff_Events__c((double) staff.getFutureEvents());
		c.setKey_Holder__c(staff.getKeyHolder());
		if (staff.getLeave() != null)
			c.setStaff_Leave_Reason__c(staff.getLeave());
		if (staff.getOccupation() != null)
			c.setOccupation__c(staff.getOccupation());
		if (staff.getStartInfo() != null)
			c.setAdditional_Details_1__c(staff.getStartInfo());
		if (staff.getTShirt() != null)
			c.setShirt_Size__c(staff.getTShirt());
		if (staff.getWhereDidYouHear() != null)
			c.setHow_you_heard_about_us__c(staff.getWhereDidYouHear());

		// Convert dates
		if (staff.getLiveScan() != null && !staff.getLiveScan().equals(""))
			c.setLiveScan_Date__c(convertDateStringToCalendar(staff.getLiveScan()));
		if (staff.getBirthdate() != null && !staff.getBirthdate().equals(""))
			c.setDate_of_Birth__c(convertDateStringToCalendar(staff.getBirthdate()));

		return c;
	}

	private boolean createAccountRecord(String firstName, String lastName, int clientID, Account account) {
		SaveResult[] saveResults = null;
		Account[] acctList = new Account[] { account };

		try {
			// Update the account records in Salesforce.com
			saveResults = connection.create(acctList);

		} catch (ConnectionException e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ACCOUNT_ERROR,
						new StudentNameModel(firstName, lastName, false), clientID,
						" for " + account.getName());
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ACCOUNT_ERROR,
						new StudentNameModel(firstName, lastName, false), clientID,
						" for " + account.getName() + ": " + e.getMessage());
			return false;
		}

		// check the returned results for any errors
		if (saveResults[0].isSuccess()) {
			sqlDb.insertLogData(LogDataModel.CREATE_SF_ACCOUNT_FOR_CLIENT, new StudentNameModel(firstName, lastName, false), clientID,
					" " + firstName + " " + lastName + ": " + account.getName());
			return true;
		} else {
			Error[] errors = saveResults[0].getErrors();
			for (int j = 0; j < errors.length; j++) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ACCOUNT_ERROR,
						new StudentNameModel(firstName, lastName, false), clientID,
						" for " + account.getName() + ": " + errors[j].getMessage());
			}
			return false;
		}
	}

	private String getStaffAccountID(StaffMemberModel staff, ArrayList<Account> sfAccounts) {

		// Check if staff member already has an account ID
		if (staff.getAccountID() != null && !staff.getAccountID().equals(""))
			return staff.getAccountID();

		// Check if student teacher or parent
		if (staff.getFirstName().startsWith("TA-") || staff.isAlsoClient()) {
			// All TA's and existing clients must already have an account
			System.out.println("Staff account error: no account for " + staff.getFullName() + ", " + staff.getClientID()
					+ ", " + staff.getSfClientID());
			return null;
		}

		// Create account family name and check whether it already exists
		String acctFamilyName = staff.getLastName() + " " + staff.getFirstName() + " Family";
		Account account = ListUtilities.findAccountByName(acctFamilyName, sfAccounts);
		if (account != null) {
			// Account name already exists
			System.out.println("Staff account error: already exists - " + staff.getFullName() + ", "
					+ staff.getClientID() + ", " + staff.getSfClientID());
			return null;
		}
		
		// New volunteer/teacher: create an account
		account = new Account();
		account.setName(acctFamilyName);
		account.setType("Family");
//		if (!createAccountRecord(staff.getFirstName(), staff.getLastName(), Integer.parseInt(staff.getClientID()), account)) {
//			System.out.println("Failed to create account for " + staff.getFullName() + ", " + staff.getClientID() + ", "
//					+ staff.getSfClientID() + ", " + acctFamilyName);
//			return null;
//		}
//
//		// Now that account has been created, need to get account from SF again
//		account = getRecords.getSalesForceAccountByName(acctFamilyName);
//		if (account == null || account.getName().equals("")) {
//			System.out.println("Failed to get account for " + staff.getFullName() + ", " + staff.getClientID() + ", "
//					+ staff.getSfClientID() + ", " + acctFamilyName);
//			return null;
//		}
//
//		// Add to SF account list
//		sfAccounts.add(account);
		System.out.println("Created account for " + staff.getFullName() + ", " + staff.getClientID() + ", "
				+ staff.getSfClientID() + ", " + acctFamilyName);
		return account.getId();
	}

	private static String parsePhone(String origPhone) {
		String phone = origPhone.trim();
		if (phone.length() < 10)
			return origPhone;

		if (phone.length() == 10 && phone.indexOf('(') == -1 && phone.indexOf('-') == -1)
			phone = "(" + phone.substring(0, 3) + ")" + " " + phone.substring(3, 6) + "-" + phone.substring(6);
		else if (phone.length() == 12 && (phone.charAt(3) == '-' || phone.charAt(3) == '.' || phone.charAt(3) == ' ')
				&& (phone.charAt(7) == '-' || phone.charAt(7) == '.' || phone.charAt(7) == ' '))
			phone = "(" + phone.substring(0, 3) + ")" + " " + phone.substring(4, 7) + "-" + phone.substring(8);
		else if (phone.length() == 13 && phone.charAt(0) == '(' && phone.charAt(4) == ')' && phone.charAt(8) == '-')
			phone = phone.substring(0, 5) + " " + phone.substring(5);

		return phone;
	}

	private static Address parseAddress(String origAddress) {
		Address mailAddr = new Address();
		String address = origAddress;

		// Replace all CR/LF characters in address
		if (address.contains("\\r") || address.contains("\\n")) {
			address = address.replace(",\\r\\n", ", ");
			address = address.replace("\\r\\n", ", ");
			address = address.replace("\\n", ", ");
		}

		// Find street by looking for next comma
		int idx = address.indexOf(',');
		if (idx <= 0)
			return null;

		mailAddr.setStreet(address.substring(0, idx).trim());
		address = address.substring(idx + 1).trim();

		// Find city by looking for next comma
		idx = address.indexOf(',');
		if (idx <= 0)
			return null;

		mailAddr.setCity(address.substring(0, idx).trim());
		address = address.substring(idx + 1).trim();

		// State may be followed by a comma or a space "CA, 92130" or "CA 92130",
		// or sometimes "CA 92130, United States".
		// So finding the first comma does not always work.
		int idx1 = address.indexOf(',');
		int idx2 = address.indexOf(' ');
		if (idx2 <= 0)
			return null;

		if (idx1 > 0) { // comma and space both exist
			if (idx1 < idx2)
				idx = idx1;
			else
				idx = idx2;
		} else // only a space exists
			idx = idx2;

		// Found state, but only handling California
		String state = address.substring(0, idx).trim();
		if (!state.equalsIgnoreCase("CA") && !state.equalsIgnoreCase("California") && !state.equalsIgnoreCase("CA.")) {
			return null;
		}

		mailAddr.setState(state);
		address = address.substring(idx + 1).trim();

		// Find zip, make sure not to include country
		if (address.length() < 5)
			return null;

		// Check that zip code is all numeric
		String zip = address.substring(0, 5);
		if (!zip.matches("\\d+"))
			return null;

		mailAddr.setPostalCode(zip);
		return mailAddr;
	}

	private static Calendar convertDateStringToCalendar(String dateString) {
		DateTime date = new DateTime(dateString);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date.getMillis());
		return cal;
	}
}
