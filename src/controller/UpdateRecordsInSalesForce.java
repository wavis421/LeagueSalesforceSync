package controller;

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
import model.LocationModel;
import model.LogDataModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.SalesForceStaffHoursModel;
import model.StaffMemberModel;
import model.StudentImportModel;
import model.StudentNameModel;

public class UpdateRecordsInSalesForce {
	private static final int MAX_NUM_UPSERT_RECORDS = 200;
	private static final int WEEKS_TO_UPDATE_WORKSHOP_GRADS = 2;
	private static final String RECORD_TYPE_ID_STUDENT = "012o000000089x0AAA";
	private static final String RECORD_TYPE_ID_ADULT = "012o000000089wzAAA";
	private static final int MAX_SALESFORCE_FIELD_LENGTH = 100;

	private MySqlDatabase sqlDb;
	private EnterpriseConnection connection;
	private GetRecordsFromSalesForce getRecords;

	private int clientUpdateCount = 0;
	private int attendanceUpsertCount = 0;
	private int staffHoursUpsertCount = 0;
	private int attendanceDeleteCount = 0;
	private boolean attendanceUpsertError = false;

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
					if (!createAccountRecord(student.getFirstName(), student.getLastName(), student.getClientID(),
							account))
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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
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

				// Ignore school clients!
				if (adult.getFullName().equals("wilson Middle School") || adult.getFullName().equals("Sample Customer")
						|| adult.getFullName().equals("barrio Logan College Institute")
						|| adult.getFirstName().startsWith("HOLIDAY"))
					continue;

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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + clientUpdateCount + " adult records processed");
	}

	// TODO: Finish this. This method is not being called yet.
	public void removeExtraContactRecords(ArrayList<StudentImportModel> pike13Clients,
			ArrayList<StaffMemberModel> pike13Staff, ArrayList<Contact> sfContacts) {

		ArrayList<String> deleteList = new ArrayList<String>();

		for (Contact c : sfContacts) {
			// Check whether contact record exists in Pike13
			if (c.getContact_Type__c() == null) {
				System.out.println("Contact record null for " + c.getFirstName() + " " + c.getLastName() + ", "
						+ c.getFront_Desk_Id__c());
				deleteList.add(c.getFront_Desk_Id__c());
			}

			else if (ListUtilities.findClientIDInPike13List(c.getFront_Desk_Id__c(), pike13Clients) == null
					&& ListUtilities.findStaffIDInList(-1, c.getFront_Desk_Id__c(), "", "", "", pike13Staff) == null) {
				System.out.println("Remove client: " + c.getFirstName() + " " + c.getLastName() + ", "
						+ c.getFront_Desk_Id__c() + ", " + c.getContact_Type__c());
				deleteList.add(c.getFront_Desk_Id__c());

			}
		}

		System.out.println("Deleted contact records: " + deleteList.size());
	}

	public void updateAttendance(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			ArrayList<AttendanceEventModel> dbAttendance, ArrayList<Contact> contacts, ArrayList<Contact> allContacts,
			ArrayList<StaffMemberModel> staffMembers) {
		ArrayList<Student_Attendance__c> recordList = new ArrayList<Student_Attendance__c>();
		ArrayList<Contact> workShopGrads = new ArrayList<Contact>();
		ArrayList<ContactModel> classGrads = new ArrayList<ContactModel>();
		ArrayList<ContactModel> repoNames = new ArrayList<ContactModel>();
		clientUpdateCount = 0;

		try {
			for (int i = 0; i < pike13Attendance.size(); i++) {
				// Add each Pike13 attendance record to SalesForce list
				SalesForceAttendanceModel inputModel = pike13Attendance.get(i);

				Contact c = ListUtilities.findClientIDInList(LogDataModel.MISSING_SF_CONTACT_FOR_ATTENDANCE,
						inputModel.getClientID(), inputModel.getFullName(), inputModel.getEventName(), contacts);
				if (c == null)
					continue;

				// Report error if event name is blank
				String locCode = null;
				if (inputModel.getEventName() == null || inputModel.getEventName().equals("")) {
					Contact cTemp = ListUtilities.findClientIDInList(-1, inputModel.getClientID(),
							inputModel.getFullName(), "", allContacts);
					if (cTemp.getContact_Type__c() != null && cTemp.getContact_Type__c().equals("Student")) {
						sqlDb.insertLogData(LogDataModel.BLANK_EVENT_NAME_FOR_ATTENDANCE,
								new StudentNameModel(inputModel.getFullName(), "", false),
								Integer.parseInt(inputModel.getClientID()),
								" on " + inputModel.getServiceDate() + ", " + inputModel.getServiceName());
					}
				} else
					locCode = ListUtilities.findLocCodeInList(inputModel.getEventName());

				// Report error if location code is invalid
				if (locCode == null) {
					// Location code not valid, report error if '@' in event name
					if (inputModel.getEventName() != null && inputModel.getEventName().contains("@"))
						sqlDb.insertLogData(LogDataModel.ATTENDANCE_LOC_CODE_INVALID,
								new StudentNameModel(inputModel.getFullName(), "", false),
								Integer.parseInt(inputModel.getClientID()), " for event " + inputModel.getEventName());

				} else if (LocationModel.findLocationCodeMatch(locCode, inputModel.getLocation()) < 0) {
					// Location code is valid, but does not match event location
					sqlDb.insertLogData(LogDataModel.ATTENDANCE_LOC_CODE_MISMATCH,
							new StudentNameModel(inputModel.getFullName(), "", false),
							Integer.parseInt(inputModel.getClientID()),
							" for event " + inputModel.getEventName() + ", " + inputModel.getLocation());
				}

				Student_Attendance__c a = new Student_Attendance__c();
				a.setContact__r(c);
				a.setEvent_Name__c(inputModel.getEventName());
				a.setService_Name__c(inputModel.getServiceName());
				if (inputModel.getEventType() != null) {
					if (inputModel.getEventType().length() > 6) {
						a.setEvent_Type__c(inputModel.getEventType().substring(0, 5));
						a.setEvent_Type_Sub__c(inputModel.getEventType().substring(6));
					} else {
						a.setEvent_Type__c(inputModel.getEventType());
						a.setEvent_Type_Sub__c("");
					}
				}
				a.setStatus__c(inputModel.getStatus());
				a.setSchedule_id__c(inputModel.getScheduleID());
				a.setVisit_Id__c(inputModel.getVisitID());
				a.setLocation__c(inputModel.getLocation());
				AttendanceEventModel attend = ListUtilities.findAttendanceEventInList(inputModel.getVisitID(),
						dbAttendance);
				if (attend != null) {
					if (!attend.getGithubComments().equals(""))
						a.setNote__c(attend.getGithubComments());
					if (attend.getRepoName() != null && !attend.getRepoName().equals("")) {
						a.setRepo_Name__c(attend.getRepoName());
						updateRepoName(repoNames, inputModel, attend.getRepoName());
					}
				}
				String newStaff = parseTeacherString(inputModel.getStaff(), a, staffMembers);
				if (newStaff.length() > MAX_SALESFORCE_FIELD_LENGTH)
					a.setStaff__c(newStaff.substring(0, MAX_SALESFORCE_FIELD_LENGTH));
				else
					a.setStaff__c(newStaff);

				a.setService_Date__c(convertDateStringToCalendar(inputModel.getServiceDate()));
				a.setService_TIme__c(inputModel.getServiceTime());

				// Update contact's Intro to Java workshop grad dates & class levels
				updateWorkshopGrad(workShopGrads, inputModel);
				updateClassLevel(classGrads, inputModel);

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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SF_ATTENDANCE_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						"");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_ATTENDANCE_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_ATTENDANCE_UPDATED, new StudentNameModel("", "", false), 0,
				", " + attendanceUpsertCount + " records processed");

		// Update modified clients to SalesForce
		if (workShopGrads.size() > 0) {
			upsertContactRecordList(workShopGrads, "WorkS grad");
			sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
					", " + clientUpdateCount + " WorkShop grad records processed");
		}
		if (classGrads.size() > 0) {
			clientUpdateCount = 0;
			upsertGradList(classGrads, allContacts);
			sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
					", " + clientUpdateCount + " Class grad records processed");
		}
		if (repoNames.size() > 0) {
			clientUpdateCount = 0;
			upsertRepoList(repoNames, allContacts);
			sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
					", " + clientUpdateCount + " Last Repo records processed");
		}
	}

	public void removeExtraAttendanceRecords(ArrayList<SalesForceAttendanceModel> pike13Attendance, String startDate,
			String endDate, ArrayList<StudentImportModel> studentList) {
		String serviceDate;
		boolean done = false;
		QueryResult queryResult;
		ArrayList<String> deleteList = new ArrayList<String>();

		if (attendanceUpsertError) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_CANCELED_ATTEND_CLEANUP, new StudentNameModel("", "", false),
					0, ", 0 records deleted: aborted due to attendance upsert error(s)");
			return;
		}

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

							StudentImportModel student = ListUtilities.findClientIDInPike13List(a.getFront_Desk_ID__c(),
									studentList);
							if (student != null)
								sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_RECORD,
										new StudentNameModel(student.getFirstName(), student.getLastName(), false),
										Integer.parseInt(a.getFront_Desk_ID__c()),
										" " + a.getVisit_Id__c() + " " + a.getEvent_Name__c() + " on " + serviceDate);
							else
								sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_RECORD,
										new StudentNameModel("", "", false), Integer.parseInt(a.getFront_Desk_ID__c()),
										" " + a.getVisit_Id__c() + " " + a.getEvent_Name__c() + " on " + serviceDate);
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
				", " + attendanceDeleteCount + " records deleted");
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
				String accountID, contactType = "Adult";

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
					contactType = "Student";
				}

				// Get account ID, create account if needed
				Contact c = ListUtilities.findClientIDInList(-1, clientID, staff.getFullName(), "", sfContacts);
				if (c == null) {
					accountID = getStaffAccountID(staff, sfAccounts);
					if (accountID == null)
						continue;
				} else
					accountID = c.getAccountId();

				// Add to list
				recordList.add(createStaffRecord(staff, firstName, clientID, accountID, contactType));
			}

			upsertContactRecordList(recordList, "staff");

		} catch (Exception e) {
			if (e.getMessage() == null || e.getMessage().equals("null")) {
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
				if (inputModel.getFullName().startsWith("Intro to Java"))
					continue;

				String staffID = ListUtilities.findStaffIDInList(LogDataModel.MISSING_PIKE13_STAFF_MEMBER,
						inputModel.getClientID(), inputModel.getFullName(), inputModel.getServiceDate(),
						inputModel.getServiceName(), pike13StaffMembers);
				if (staffID == null)
					continue;

				Contact c = ListUtilities.findClientIDInList(LogDataModel.MISSING_SALES_FORCE_STAFF_MEMBER, staffID,
						inputModel.getFullName(), "", contacts);
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
				if (inputModel.getServiceCategory() != null) {
					if (inputModel.getServiceCategory().length() > 6) {
						h.setEvent_type__c(inputModel.getServiceCategory().substring(0, 5));
						h.setEvent_type_sub__c(inputModel.getServiceCategory().substring(6));
					} else {
						h.setEvent_type__c(inputModel.getServiceCategory());
						h.setEvent_type_sub__c("");
					}
				}

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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR, new StudentNameModel("", "", false),
						0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR, new StudentNameModel("", "", false),
						0, ": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				// If client ID is numeric, then use this in error log
				int clientID = 0;
				if (records[i].getFront_Desk_Id__c().matches("\\d+"))
					clientID = Integer.parseInt(records[i].getFront_Desk_Id__c());

				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR,
							new StudentNameModel(records[i].getFirstName(), records[i].getLastName(), false), clientID,
							": " + errors[j].getMessage());
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
			attendanceUpsertError = true;
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR,
						new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR,
						new StudentNameModel("", "", false), 0, ": " + e.getMessage());
			return;
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				// If client ID is numeric, then use this in error log
				Contact c = records[i].getContact__r();
				int clientID = 0;
				if (c != null && c.getFront_Desk_Id__c() != null && c.getFront_Desk_Id__c().matches("\\d+"))
					clientID = Integer.parseInt(c.getFront_Desk_Id__c());

				attendanceUpsertError = true;
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR,
							new StudentNameModel("", "", false), clientID, ": " + errors[j].getMessage());
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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_ERROR,
						new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_ERROR,
						new StudentNameModel("", "", false), 0, ": " + e.getMessage());
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
			} else
				attendanceDeleteCount++;
		}
	}

	private void upsertStaffHoursRecords(Staff_Hours__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("schedule_client_ID__c", records);

		} catch (ConnectionException e) {
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR,
						new StudentNameModel("", "", false), 0, "");
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR,
						new StudentNameModel("", "", false), 0, ": " + e.getMessage());
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

	private void updateWorkshopGrad(ArrayList<Contact> workShopGrads, SalesForceAttendanceModel inputModel) {
		// Check for null lists
		if (inputModel.getEventName() == null || inputModel.getStatus() == null || inputModel.getServiceDate() == null)
			return;

		// If event is Intro to Java Workshop and is completed, add to grad date list
		if (inputModel.getEventName().contains("Intro to Java Workshop") && inputModel.getStatus().equals("completed")
				&& inputModel.getServiceDate().compareTo(getDateInPastByWeeks(WEEKS_TO_UPDATE_WORKSHOP_GRADS)) > 0) {
			Contact gradContact = new Contact();
			Calendar newGradCal = convertDateStringToCalendar(inputModel.getServiceDate());
			gradContact.setFront_Desk_Id__c(inputModel.getClientID());
			gradContact.setWorkshop_Grad_Date__c(newGradCal);

			// Add record; if already in grad list, remove and replace with later grad date
			Contact dupContact = ListUtilities.findClientIDInList(-1, inputModel.getClientID(),
					inputModel.getFullName(), "", workShopGrads);

			if (dupContact == null) {
				// Not already in list, so add
				workShopGrads.add(gradContact);

			} else if (dupContact.getWorkshop_Grad_Date__c().compareTo(newGradCal) < 0) {
				// This client is already in list and older date, so update wshop grad date
				dupContact.setWorkshop_Grad_Date__c(newGradCal);
			}
		}
	}

	private void updateClassLevel(ArrayList<ContactModel> graduates, SalesForceAttendanceModel inputModel) {
		// If event is a regular class and is completed, add to grad list
		if (inputModel.getEventName() != null && inputModel.getEventName().length() > 2
				&& inputModel.getEventName().charAt(0) >= '0' && inputModel.getEventName().charAt(0) <= '9'
				&& inputModel.getEventName().charAt(1) == '@' && inputModel.getStatus().equals("completed")) {

			ContactModel dupGrad = findFieldInContactList(inputModel.getClientID(), graduates);

			if (dupGrad == null) {
				// Not already in list, so add
				graduates.add(new ContactModel(inputModel.getClientID(), inputModel.getEventName(),
						inputModel.getServiceDate()));

			} else if (inputModel.getServiceDate().compareTo(dupGrad.getServiceDate()) > 0) {
				// This client is already in list and date is later, so update
				dupGrad.setStringField(inputModel.getEventName());
				dupGrad.setServiceDate(inputModel.getServiceDate());
			}
		}
	}

	private void upsertGradList(ArrayList<ContactModel> gradList, ArrayList<Contact> contactList) {
		ArrayList<Contact> gradContacts = new ArrayList<Contact>();

		for (ContactModel grad : gradList) {
			// Get last level from SalesForce
			String lastLevel = "";
			Contact contact = ListUtilities.findClientIDInList(-1, grad.getClientID(), "", "", contactList);
			if (contact != null && contact.getLast_Class_Level__c() != null)
				lastLevel = contact.getLast_Class_Level__c();

			// Update if different
			if (lastLevel.equals("") || !lastLevel.equals(grad.getStringField().substring(0, 1))) {
				// Create contact
				Contact c = new Contact();
				c.setFront_Desk_Id__c(grad.getClientID());
				c.setCurrent_Level__c(grad.getStringField());

				gradContacts.add(c);
			}
		}
		if (gradContacts.size() > 0) {
			upsertContactRecordList(gradContacts, "Class grad");
		}
	}

	private void updateRepoName(ArrayList<ContactModel> repoList, SalesForceAttendanceModel inputModel,
			String repoName) {
		// Store latest repo name for this student
		ContactModel dupContact = findFieldInContactList(inputModel.getClientID(), repoList);

		if (dupContact == null) {
			// Not already in list, so add
			repoList.add(new ContactModel(inputModel.getClientID(), repoName, inputModel.getServiceDate()));

		} else if (inputModel.getServiceDate().compareTo(dupContact.getServiceDate()) > 0) {
			// This client is already in list and date is later, so update
			dupContact.setStringField(repoName);
			dupContact.setServiceDate(inputModel.getServiceDate());
		}
	}

	private void upsertRepoList(ArrayList<ContactModel> repoList, ArrayList<Contact> contactList) {
		ArrayList<Contact> repoContacts = new ArrayList<Contact>();

		for (ContactModel repo : repoList) {
			// Create contact
			Contact c = new Contact();
			c.setFront_Desk_Id__c(repo.getClientID());
			c.setLast_Repo_Commit__c(repo.getStringField());

			repoContacts.add(c);
		}
		if (repoContacts.size() > 0) {
			upsertContactRecordList(repoContacts, "Repo Names");
		}
	}

	private Contact createContactRecord(boolean adult, StudentImportModel contact, Account account) {
		// Create contact and add fields
		Contact c = new Contact();

		c.setAccountId(account.getId());
		c.setFront_Desk_Id__c(String.valueOf(contact.getClientID()));
		c.setFirstName(contact.getFirstName());
		c.setLastName(contact.getLastName());
		if (adult) {
			c.setContact_Type__c("Adult");
			c.setRecordTypeId(RECORD_TYPE_ID_ADULT);
		} else {
			c.setContact_Type__c("Student");
			c.setRecordTypeId(RECORD_TYPE_ID_STUDENT);
			updateFamilyEmail(contact, c);
		}
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
		if (contact.getPassOnFile() != null && !contact.getPassOnFile().equals("")) {
			if (contact.getPassOnFile().length() > MAX_SALESFORCE_FIELD_LENGTH)
				c.setPlan__c(contact.getPassOnFile().substring(0, MAX_SALESFORCE_FIELD_LENGTH));
			else
				c.setPlan__c(contact.getPassOnFile());
		}
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

	private Contact createStaffRecord(StaffMemberModel staff, String firstName, String clientID, String accountID,
			String contactType) {
		// Create contact and add fields
		Contact c = new Contact();

		c.setAccountId(accountID);
		c.setFront_Desk_Id__c(clientID);
		c.setFirstName(firstName);
		c.setLastName(staff.getLastName());
		c.setContact_Type__c(contactType);
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
			if (e.getMessage() == null || e.getMessage().equals("null")) {
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ACCOUNT_ERROR,
						new StudentNameModel(firstName, lastName, false), clientID, " for " + account.getName());
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ACCOUNT_ERROR,
						new StudentNameModel(firstName, lastName, false), clientID,
						" for " + account.getName() + ": " + e.getMessage());
			return false;
		}

		// check the returned results for any errors
		if (saveResults[0].isSuccess()) {
			sqlDb.insertLogData(LogDataModel.CREATE_SALES_FORCE_ACCOUNT,
					new StudentNameModel(firstName, lastName, false), clientID,
					" for " + firstName + " " + lastName + ": " + account.getName());
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

		// Check if student-teacher or parent
		if (staff.getFirstName().startsWith("TA-") || staff.isAlsoClient()) {
			// All TA's and existing clients must already have an account
			sqlDb.insertLogData(LogDataModel.MISSING_ACCOUNT_FOR_TA_OR_PARENT,
					new StudentNameModel(staff.getFullName(), "", false), 0, " " + staff.getFullName());
			return null;
		}

		// Create account family name and check whether it already exists
		String acctFamilyName = staff.getLastName() + " " + staff.getFirstName() + " Family";
		Account account = ListUtilities.findAccountByName(acctFamilyName, sfAccounts);
		if (account != null) {
			// Account name already exists
			return account.getId();
		}

		// New volunteer/teacher: create an account
		account = new Account();
		account.setName(acctFamilyName);
		account.setType("Family");
		if (!createAccountRecord(staff.getFirstName(), staff.getLastName(), Integer.parseInt(staff.getClientID()),
				account))
			return null;

		// Now that account has been created, need to get account from SF again
		account = getRecords.getSalesForceAccountByName(acctFamilyName);
		if (account == null || account.getName().equals("")) {
			sqlDb.insertLogData(LogDataModel.UPSERTED_ACCOUNT_RETRIEVAL_ERROR,
					new StudentNameModel(staff.getFullName(), "", false), 0, " for " + acctFamilyName);
			return null;
		}

		// Add to SF account list
		sfAccounts.add(account);
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

	private String parseTeacherString(String teachers, Student_Attendance__c attend,
			ArrayList<StaffMemberModel> staffList) {

		// Clear the old values in SalesForce record
		attend.setTeacher_1__c("");
		attend.setTeacher_2__c("");
		attend.setTeacher_3_Vol__c("");
		attend.setTeacher_4_Vol__c("");
		
		// Check for missing teacher record
		if (teachers == null || teachers.equals(""))
			return teachers;

		// Parse teachers into CSV
		String newTeachers = "";
		String[] values = teachers.split("\\s*,\\s*");

		for (int i = 0; i < values.length; i++) {
			// Ignore unwanted field values
			if (values[i].startsWith("League Admin") || values[i].startsWith("Summer Prog")
					|| values[i].startsWith("Intro to Java") || values[i].startsWith("Padres"))
				continue;

			// Add teacher to new teachers string
			if (newTeachers.length() > 0)
				newTeachers += ", ";
			newTeachers += values[i];

			// Only add staff members
			StaffMemberModel staff = ListUtilities.findStaffNameInList(values[i], staffList);
			if (staff == null)
				continue;

			if (staff.getCategory().equals("Teaching Staff")) {
				if (attend.getTeacher_1__c().equals(""))
					attend.setTeacher_1__c(values[i]);
				else if (attend.getTeacher_2__c().equals(""))
					attend.setTeacher_2__c(values[i]);

			} else if (staff.getCategory().equals("Vol Teacher")) {
				if (attend.getTeacher_3_Vol__c().equals(""))
					attend.setTeacher_3_Vol__c(values[i]);
				else if (attend.getTeacher_4_Vol__c().equals(""))
					attend.setTeacher_4_Vol__c(values[i]);
			}
		}

		return newTeachers;
	}

	private static Calendar convertDateStringToCalendar(String dateString) {
		DateTime date = new DateTime(dateString);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date.getMillis());
		return cal;
	}

	private static String getDateInPastByWeeks(int minusNumWeeks) {
		DateTime date = new DateTime().minusWeeks(minusNumWeeks);
		return date.toString("yyyy-MM-dd");
	}

	private static void updateFamilyEmail(StudentImportModel model, Contact c) {
		String emails = "";

		// Concatenate student email, account manager emails & emergency email
		if (model.getEmail() != null && !model.getEmail().equals(""))
			emails += model.getEmail();
		if (model.getAccountMgrEmails() != null && !model.getAccountMgrEmails().equals("")
				&& !emails.contains(model.getAccountMgrEmails())) {
			if (!emails.equals(""))
				emails += ", ";
			emails += model.getAccountMgrEmails();
		}
		if (model.getEmergContactEmail() != null && !model.getEmergContactEmail().equals("")
				&& !emails.contains(model.getEmergContactEmail())) {
			if (!emails.equals(""))
				emails += ", ";
			emails += model.getEmergContactEmail();
		}

		// Now add email string to SalesForce contact
		if (!emails.equals(""))
			c.setFamily_Email__c(emails);
	}

	/*
	 * ~~~~ Contact Model Class used as an intermediary data structure ~~~~
	 */
	private class ContactModel {
		// Model for storing temp data being upserted to SalesForce Contacts
		String clientID, stringField, serviceDate;

		ContactModel(String clientID, String field1, String serviceDate) {
			this.clientID = clientID;
			this.stringField = field1;
			this.serviceDate = serviceDate;
		}

		public String getClientID() {
			return clientID;
		}

		public String getStringField() {
			return stringField;
		}

		public String getServiceDate() {
			return serviceDate;
		}

		public void setStringField(String field1) {
			this.stringField = field1;
		}

		public void setServiceDate(String serviceDate) {
			this.serviceDate = serviceDate;
		}
	}

	public static ContactModel findFieldInContactList(String clientID, ArrayList<ContactModel> contactList) {
		for (ContactModel c : contactList) {
			if (c.getClientID().equals(clientID)) {
				return c;
			}
		}
		return null;
	}
}
