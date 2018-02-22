package wsc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

import org.joda.time.DateTime;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.DeleteResult;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.Error;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.UpsertResult;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.soap.enterprise.sobject.Staff_Hours__c;
import com.sforce.soap.enterprise.sobject.Student_Attendance__c;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import controller.Pike13Api;
import model.LogDataModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.SalesForceStaffHoursModel;
import model.StaffMemberModel;
import model.StudentImportModel;
import model.StudentNameModel;

public class SalesForceApi {

	private static final String SALES_FORCE_USERNAME = "leaguebot@jointheleague.org";
	private static final String SALES_FORCE_PASSWORD = readFile("./sfPassword.txt");
	private static final String AWS_PASSWORD = readFile("./awsPassword.txt");
	private static final int MAX_NUM_UPSERT_RECORDS = 200;
	private static final int PAST_DATE_RANGE_INTERVAL_IN_DAYS = 30;
	private static final int FUTURE_DATE_RANGE_INTERVAL_IN_DAYS = 90;

	private static EnterpriseConnection connection;
	private static MySqlDatabase sqlDb;
	private static int clientUpdateCount = 0;
	private static int attendanceUpsertCount = 0;
	private static int staffHoursUpsertCount = 0;
	private static String startDate, endDate, today;

	public static void main(String[] args) {
		// Connect to MySql database for logging status/errors
		sqlDb = new MySqlDatabase(AWS_PASSWORD, MySqlDatabase.SALES_FORCE_SYNC_SSH_PORT);
		if (!sqlDb.connectDatabase()) {
			// TODO: Handle this error
			System.out.println("Failed to connect to MySql database");
			System.exit(0);
		}

		// Set start/end date to +/- 30 days
		DateTime t = new DateTime();
		today = t.toString("yyyy-MM-dd");
		startDate = t.minusDays(PAST_DATE_RANGE_INTERVAL_IN_DAYS).toString("yyyy-MM-dd");
		endDate = t.plusDays(FUTURE_DATE_RANGE_INTERVAL_IN_DAYS).toString("yyyy-MM-dd");
		sqlDb.insertLogData(LogDataModel.STARTING_SALES_FORCE_IMPORT, new StudentNameModel("", "", false), 0,
				" from " + startDate + " to " + endDate + " ***");

		// Connect to Pike13
		String pike13Token = readFile("./pike13Token.txt");
		Pike13Api pike13Api = new Pike13Api(sqlDb, pike13Token);

		// Connect to SalesForce
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(SALES_FORCE_USERNAME);
		config.setPassword(SALES_FORCE_PASSWORD);
		config.setTraceMessage(false);

		try {
			connection = Connector.newConnection(config);

		} catch (ConnectionException e1) {
			exitProgram(LogDataModel.SALES_FORCE_CONNECTION_ERROR, e1.getMessage());
		}

		// Get SF contacts and store in list
		ArrayList<Contact> sfContactList = getSalesForceContacts();

		// Get Pike13 clients and upsert to Salesforce
		ArrayList<StudentImportModel> pike13ContactList = pike13Api.getClientsForSfImport();
		updateClients(pike13ContactList, sfContactList);

		// Update attendance records
		ArrayList<SalesForceAttendanceModel> sfAttendance = pike13Api.getSalesForceAttendance(startDate, endDate);
		updateAttendance(sfAttendance, sfContactList);

		// Delete canceled attendance records
		removeExtraAttendanceRecords(sfAttendance, startDate, endDate);

		// Update Staff Hours records
		ArrayList<StaffMemberModel> sfStaffMembers = pike13Api.getSalesForceStaffMembers();
		ArrayList<SalesForceStaffHoursModel> sfStaffHours = pike13Api.getSalesForceStaffHours(startDate, today);
		updateStaffHours(sfStaffMembers, sfStaffHours, sfContactList);
		getStaffHours(); // temporary, for debug

		exitProgram(-1, null); // -1 indicates no error
	}

	private static ArrayList<Contact> getSalesForceContacts() {
		ArrayList<Contact> contactsList = new ArrayList<Contact>();

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c FROM Contact WHERE Front_Desk_ID__c != null "
							+ "AND (Record_Type__c = 'Student' OR Teacher__c = 'Active' OR Teacher_Student__c = 'Active' "
							+ "OR Volunteer__c = 'Active')");
			if (queryResults.getSize() > 0) {
				for (int i = 0; i < queryResults.getRecords().length; i++) {
					// Add contact to list
					contactsList.add((Contact) queryResults.getRecords()[i]);
				}
			}

		} catch (Exception e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_CONTACTS_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
		}

		return contactsList;
	}

	private static void updateClients(ArrayList<StudentImportModel> pike13Clients, ArrayList<Contact> sfContacts) {
		ArrayList<Contact> recordList = new ArrayList<Contact>();

		try {
			for (int i = 0; i < pike13Clients.size(); i++) {
				// Add each Pike13 client record to SalesForce list
				StudentImportModel model = pike13Clients.get(i);

				// Ignore school clients!
				if ((model.getFirstName().equals("gompers") && model.getLastName().equals("Prep"))
						|| (model.getFirstName().equals("wilson") && model.getLastName().equals("Middle School"))
						|| model.getFirstName().equals("barrio Logan"))
					continue;

				// Currently only updating existing clients, so ignore if not in SalesForce
				Contact contactMatch = findClientIDInList(LogDataModel.MISSING_SF_CONTACT_FOR_CLIENT_IMPORT,
						String.valueOf(model.getClientID()), model.getFirstName() + " " + model.getLastName(), sfContacts);
				if (contactMatch == null)
					continue;

				// Create contact and fill in all fields
				Contact c = new Contact();
				c.setFront_Desk_Id__c(String.valueOf(model.getClientID()));
				if (model.getEmail() != null)
					c.setEmail(model.getEmail());
				if (model.getMobilePhone() != null)
					c.setMobilePhone(model.getMobilePhone());
				if (model.getHomePhone() != null)
					c.setPhone(model.getHomePhone());
				if (model.getAddress() != null)
					c.setFull_Address__c(model.getAddress());
				c.setPast_Events__c((double) model.getCompletedVisits());
				c.setFuture_Events__c((double) model.getFutureVisits());
				c.setSigned_Waiver__c(model.isSignedWaiver());
				c.setMembership__c(model.getMembership());
				if (model.getPassOnFile() != null)
					c.setPlan__c(model.getPassOnFile());
				if (model.getHomeLocAsString() != null)
					c.setHome_Location_Long__c(model.getHomeLocAsString());
				if (model.getSchoolName() != null)
					c.setCurrent_School__c(model.getSchoolName());
				if (model.gettShirtSize() != null)
					c.setShirt_Size__c(model.gettShirtSize());
				if (model.getGenderString() != null)
					c.setGender__c(model.getGenderString());
				if (model.getEmergContactName() != null)
					c.setEmergency_Name__c(model.getEmergContactName());
				if (model.getEmergContactPhone() != null)
					c.setEmergency_Phone__c(model.getEmergContactPhone());
				if (model.getEmergContactEmail() != null)
					c.setEmergency_Email__c(model.getEmergContactEmail());
				if (model.getCurrGrade() != null)
					c.setGrade__c(model.getCurrGrade());
				if (model.getHearAboutUs() != null)
					c.setHow_you_heard_about_us__c(model.getHearAboutUs());
				if (model.getGradYearString() != null)
					c.setGrad_Year__c(model.getGradYearString());
				if (model.getWhoToThank() != null)
					c.setWho_can_we_thank__c(model.getWhoToThank());
				if (model.getGithubName() != null)
					c.setGIT_HUB_acct_name__c(model.getGithubName());
				if (model.getGrantInfo() != null)
					c.setGrant_Information__c(model.getGrantInfo());
				c.setLeave_Reason__c(model.getLeaveReason());
				c.setStop_Email__c(model.isStopEmail());
				c.setScholarship__c(model.isFinancialAid());
				if (model.getFinancialAidPercent() != null)
					c.setScholarship_Percentage__c(model.getFinancialAidPercent());
				if (model.getBirthDate() != null && !model.getBirthDate().equals(""))
					c.setDate_of_Birth__c(convertDateStringToCalendar(model.getBirthDate()));

				// TODO: Remaining fields not yet processed: acct mgr for key

				recordList.add(c);
			}

			// Copy up to 200 records to array at a time (max allowed)
			Contact[] recordArray;
			int numRecords = recordList.size();
			int remainingRecords = numRecords;
			int arrayIdx = 0;

			System.out.println(numRecords + " Pike13 client records");
			if (numRecords > MAX_NUM_UPSERT_RECORDS)
				recordArray = new Contact[MAX_NUM_UPSERT_RECORDS];
			else
				recordArray = new Contact[numRecords];

			for (int i = 0; i < numRecords; i++) {
				recordArray[arrayIdx] = recordList.get(i);

				arrayIdx++;
				if (arrayIdx == MAX_NUM_UPSERT_RECORDS) {
					updateClientRecords(recordArray);
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
				updateClientRecords(recordArray);

		} catch (Exception e) {
			if (e.getMessage() == null)
				e.printStackTrace();
			else
				sqlDb.insertLogData(LogDataModel.SF_CLIENT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SF_CLIENTS_UPDATED, new StudentNameModel("", "", false), 0,
				", " + clientUpdateCount + " records processed");
	}

	private static void updateAttendance(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			ArrayList<Contact> contacts) {
		ArrayList<Student_Attendance__c> recordList = new ArrayList<Student_Attendance__c>();

		try {
			for (int i = 0; i < pike13Attendance.size(); i++) {
				// Add each Pike13 attendance record to SalesForce list
				SalesForceAttendanceModel inputModel = pike13Attendance.get(i);

				Contact c = findClientIDInList(LogDataModel.MISSING_SF_CONTACT_FOR_ATTENDANCE, inputModel.getClientID(),
						null, contacts);
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

				DateTime date = new DateTime(inputModel.getServiceDate());
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(date.getMillis());
				a.setService_Date__c(cal);
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
			if (e.getMessage() == null)
				e.printStackTrace();
			else
				sqlDb.insertLogData(LogDataModel.SF_ATTENDANCE_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_ATTENDANCE_UPDATED, new StudentNameModel("", "", false), 0,
				", " + attendanceUpsertCount + " records processed");
	}

	private static void removeExtraAttendanceRecords(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			String startDate, String endDate) {
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
						if (!findVisitIdInList(a.getVisit_Id__c(), pike13Attendance)) {
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

	private static ArrayList<Staff_Hours__c> getStaffHours() {
		ArrayList<Staff_Hours__c> staffHoursList = new ArrayList<Staff_Hours__c>();

		try {
			QueryResult queryResults = connection.query(
					"SELECT Schedule_ID__c, Front_Desk__c, Service__c, Event_Name__c, Service_Date__c, Service_Time__c, "
							+ "Hours__c, Location_Full__c, Present__c, Absent__c, Excused__c, schedule_client_ID__c "
							+ "FROM Staff_Hours__c WHERE Service_Date__c >= " + startDate + " AND Service_Date__c <= "
							+ today + " ORDER BY Service_Date__c DESC, Service_Time__c DESC");
			System.out.println(queryResults.getSize() + " Staff Hours records in SalesForce");

		} catch (Exception e) {
			if (e.getMessage() == null)
				e.printStackTrace();
			else
				sqlDb.insertLogData(LogDataModel.SF_STAFF_HOURS_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
		}

		return staffHoursList;
	}

	private static void updateStaffHours(ArrayList<StaffMemberModel> pike13StaffMembers,
			ArrayList<SalesForceStaffHoursModel> pike13StaffHours, ArrayList<Contact> contacts) {
		ArrayList<Staff_Hours__c> recordList = new ArrayList<Staff_Hours__c>();

		System.out.println(pike13StaffHours.size() + " Staff Hour records from Pike13");

		try {
			for (int i = 0; i < pike13StaffHours.size(); i++) {
				// Add each Pike13 staff hours record to SalesForce list
				SalesForceStaffHoursModel inputModel = pike13StaffHours.get(i);

				String staffID = findStaffIDInList(inputModel.getClientID(), pike13StaffMembers);
				if (staffID == null)
					continue;

				Contact c = findClientIDInList(LogDataModel.MISSING_SALES_FORCE_STAFF_MEMBER, staffID,
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

				DateTime date = new DateTime(inputModel.getServiceDate());
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(date.getMillis());
				h.setService_Date__c(cal);
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

	private static void updateClientRecords(Contact[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Front_Desk_ID__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_CLIENTS_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
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

	private static void upsertAttendanceRecords(Student_Attendance__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Visit_Id__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_ATTENDANCE_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
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

	private static void deleteAttendanceRecords(String[] records) {
		DeleteResult[] deleteResults = null;

		try {
			// Update the records in Salesforce.com
			deleteResults = connection.delete(records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_DELETE_ATTENDANCE_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
		}

		if (deleteResults != null) {
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
	}

	private static void upsertStaffHoursRecords(Staff_Hours__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("schedule_client_ID__c", records);

		} catch (ConnectionException e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR, new StudentNameModel("", "", false),
					0, ": " + e.getMessage());
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					sqlDb.insertLogData(LogDataModel.SALES_FORCE_UPSERT_STAFF_HOURS_ERROR,
							new StudentNameModel("", "", false), 0,
							" (" + records[i].getSchedule_ID__c() + "): " + errors[j].getMessage());
				}
			} else
				staffHoursUpsertCount++;
		}
	}

	private static Contact findClientIDInList(int errorCode, String clientID, String clientName,
			ArrayList<Contact> contactList) {
		for (Contact c : contactList) {
			if (c.getFront_Desk_Id__c().equals(clientID)) {
				return c;
			}
		}
		if (clientName == null || clientName.startsWith("null"))
			sqlDb.insertLogData(errorCode, new StudentNameModel("", "", false), Integer.parseInt(clientID),
					", ClientID " + clientID);
		else
			sqlDb.insertLogData(errorCode, new StudentNameModel("", "", false), Integer.parseInt(clientID),
					", ClientID " + clientID + " " + clientName);
		return null;
	}

	private static boolean findVisitIdInList(String visitID, ArrayList<SalesForceAttendanceModel> attendanceList) {
		for (SalesForceAttendanceModel a : attendanceList) {
			if (a.getVisitID().equals(visitID)) {
				return true;
			}
		}
		return false;
	}

	private static String findStaffIDInList(String clientID, ArrayList<StaffMemberModel> staffList) {
		for (StaffMemberModel s : staffList) {
			if (s.getClientID().equals(clientID)) {
				return s.getStaffID();
			}
		}
		sqlDb.insertLogData(LogDataModel.MISSING_PIKE13_STAFF_MEMBER, new StudentNameModel("", "", false),
				Integer.parseInt(clientID), " for ClientID " + clientID);
		return null;
	}

	private static Calendar convertDateStringToCalendar(String dateString) {
		DateTime date = new DateTime(dateString);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date.getMillis());
		return cal;
	}

	private static String readFile(String filename) {
		try {
			File file = new File(filename);
			FileInputStream fis = new FileInputStream(file);

			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();

			return new String(data, "UTF-8");

		} catch (IOException e) {
			// Do nothing if file is not there
		}
		return "";
	}

	private static void exitProgram(int errorCode, String errorMessage) {
		if (errorCode == -1) {
			// Success
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_IMPORT_COMPLETE, new StudentNameModel("", "", false), 0,
					" from " + startDate + " to " + endDate + " ***");
		} else {
			// Failure
			sqlDb.insertLogData(errorCode, new StudentNameModel("", "", false), 0, ": " + errorMessage);
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_IMPORT_ABORTED, new StudentNameModel("", "", false), 0,
					" from " + startDate + " to " + endDate + " ***");
		}
		sqlDb.disconnectDatabase();
		System.exit(0);
	}
}