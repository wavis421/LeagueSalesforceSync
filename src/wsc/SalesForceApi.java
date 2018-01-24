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
import com.sforce.soap.enterprise.sobject.Student_Attendance__c;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import controller.Pike13Api;
import model.LogDataModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.StudentNameModel;

public class SalesForceApi {

	private static final String SALES_FORCE_USERNAME = "wendy.avis@jointheleague.org";
	private static final String SALES_FORCE_PASSWORD = readFile("./sfPassword.txt");
	private static final String AWS_PASSWORD = readFile("./awsPassword.txt");
	private static final int MAX_NUM_UPSERT_RECORDS = 200;
	private static final int DATE_RANGE_INTERVAL_IN_DAYS = 30;

	private static EnterpriseConnection connection;
	private static MySqlDatabase sqlDb;
	private static int upsertCount = 0;

	public static void main(String[] args) {
		// Connect to MySql database for logging status/errors
		sqlDb = new MySqlDatabase(AWS_PASSWORD, MySqlDatabase.STUDENT_IMPORT_SSH_PORT);
		if (!sqlDb.connectDatabase()) {
			// TODO: Handle this error
			System.out.println("Failed to connect to MySql database");
			System.exit(0);
		}

		// Set start/end date to +/- 30 days
		DateTime today = new DateTime();
		String startDate = today.minusDays(DATE_RANGE_INTERVAL_IN_DAYS).toString("yyyy-MM-dd").substring(0, 10);
		String endDate = today.plusDays(DATE_RANGE_INTERVAL_IN_DAYS).toString("yyyy-MM-dd").substring(0, 10);
		sqlDb.insertLogData(LogDataModel.STARTING_SALES_FORCE_IMPORT, new StudentNameModel("", "", false), 0,
				" from " + startDate + " to " + endDate + " ***");

		// Connect to Pike13
		String pike13Token = readFile("./pike13Token.txt");
		Pike13Api pike13Api = new Pike13Api(sqlDb, pike13Token);

		// Connect to Sales Force
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(SALES_FORCE_USERNAME);
		config.setPassword(SALES_FORCE_PASSWORD);
		config.setTraceMessage(false);

		try {
			connection = Connector.newConnection(config);

		} catch (ConnectionException e1) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_CONNECTION_ERROR, new StudentNameModel("", "", false), 0,
					": " + e1.getMessage());
			sqlDb.disconnectDatabase();
			System.exit(0);
		}

		// Get contacts and store in list
		ArrayList<Contact> contactList = getContacts();

		// Update attendance records
		ArrayList<SalesForceAttendanceModel> sfAttendance = pike13Api.getSalesForceAttendance(startDate, endDate);
		updateAttendance(sfAttendance, contactList);

		// Delete future cancelled attendance records
		removeExtraAttendanceRecords(sfAttendance, today.toString("yyyy-MM-dd"), endDate);

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_IMPORT_COMPLETE, new StudentNameModel("", "", false), 0,
				" from " + startDate + " to " + endDate + " ***");
		sqlDb.disconnectDatabase();
		System.exit(0);
	}

	private static ArrayList<Contact> getContacts() {
		ArrayList<Contact> contactsList = new ArrayList<Contact>();

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c FROM Contact WHERE Front_Desk_ID__c != null "
							+ "AND Record_Type__c = 'Student'");
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

	private static void updateAttendance(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			ArrayList<Contact> contacts) {
		ArrayList<Student_Attendance__c> recordList = new ArrayList<Student_Attendance__c>();

		try {
			for (int i = 0; i < pike13Attendance.size(); i++) {
				// Add each Pike13 attendance record to Sales Force list
				SalesForceAttendanceModel inputModel = pike13Attendance.get(i);

				Contact c = findContactInList(inputModel.getClientID(), contacts);
				if (c == null)
					continue;

				Student_Attendance__c a = new Student_Attendance__c();
				a.setContact__r(c);
				a.setEvent_Name__c(inputModel.getEventName());
				a.setService_Name__c(inputModel.getServiceName());
				Calendar cal = Calendar.getInstance();
				String date = inputModel.getServiceDate();
				cal.set(Calendar.YEAR, Integer.parseInt(date.substring(0, 4)));
				cal.set(Calendar.MONTH, Integer.parseInt(date.substring(5, 7)) - 1);
				cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(date.substring(8, 10)));
				a.setService_Date__c(cal);
				a.setService_TIme__c(inputModel.getServiceTime());
				a.setStatus__c(inputModel.getStatus());
				a.setSchedule_id__c(inputModel.getScheduleID());
				a.setVisit_Id__c(inputModel.getVisitID());
				a.setLocation__c(inputModel.getLocation());
				a.setStaff__c(inputModel.getStaff());
				recordList.add(a);
			}

			// Copy up to 200 records to array at a time (max allowed)
			Student_Attendance__c[] recordArray;
			int numRecords = recordList.size();
			int remainingRecords = numRecords;
			int arrayIdx = 0;

			System.out.println(numRecords + " attendance records");
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
			e.printStackTrace();
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_ATTENDANCE_UPDATED, new StudentNameModel("", "", false), 0,
				", " + upsertCount + " records processed");
	}

	private static void removeExtraAttendanceRecords(ArrayList<SalesForceAttendanceModel> pike13Attendance,
			String startDate, String endDate) {
		String serviceDate;
		boolean done = false;
		QueryResult queryResult;
		ArrayList<String> deleteList = new ArrayList<String>();

		try {
			queryResult = connection.query("SELECT Id, Visit_Id__c, Service_Date__c, Front_Desk_ID__c, Event_Name__c "
					+ "FROM Student_Attendance__c WHERE Status__c = 'Enrolled' AND Service_Date__c > " + startDate
					+ " AND Service_Date__c <= " + endDate + " ORDER BY Visit_Id__c ASC");
			System.out.println(queryResult.getSize() + " future attendance records.");

			if (queryResult.getSize() > 0) {
				while (!done) {
					SObject[] records = queryResult.getRecords();
					for (int i = 0; i < records.length; i++) {
						// Check whether attendance record exists in Pike13
						Student_Attendance__c a = (Student_Attendance__c) records[i];
						if (!findVisitIdInList(a.getVisit_Id__c(), pike13Attendance)) {
							// Record not found, so delete
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

			} else
				System.out.println("No future attendance records found!");

		} catch (Exception e) {
			e.printStackTrace();
		}

		sqlDb.insertLogData(LogDataModel.SALES_FORCE_FUTURE_ATTEND_CLEANUP, new StudentNameModel("", "", false), 0,
				", " + deleteList.size() + " records deleted");
	}

	private static void upsertAttendanceRecords(Student_Attendance__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Visit_Id__c", records);
			upsertCount += records.length;

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
			}
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

	private static Contact findContactInList(String clientID, ArrayList<Contact> contactList) {
		for (Contact c : contactList) {
			if (c.getFront_Desk_Id__c().equals(clientID)) {
				return c;
			}
		}
		sqlDb.insertLogData(LogDataModel.MISSING_SALES_FORCE_CONTACT, new StudentNameModel("", "", false),
				Integer.parseInt(clientID), " for ClientID " + clientID);
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
}