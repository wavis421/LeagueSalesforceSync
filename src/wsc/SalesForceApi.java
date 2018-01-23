package wsc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.Error;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.UpsertResult;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.Student_Attendance__c;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import controller.Pike13Api;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;

public class SalesForceApi {

	private static final String SALES_FORCE_USERNAME = "wendy.avis@jointheleague.org";
	private static final String SALES_FORCE_PASSWORD = readFile("./sfPassword.txt");
	private static final String AWS_PASSWORD = readFile("./awsPassword.txt");
	private static final int MAX_NUM_UPSERT_RECORDS = 200;

	private static EnterpriseConnection connection;
	private static int upsertCount = 0;

	public static void main(String[] args) {
		// Connect to MySql database for logging status/errors
		MySqlDatabase sqlDb = new MySqlDatabase(AWS_PASSWORD, MySqlDatabase.STUDENT_IMPORT_SSH_PORT);
		if (!sqlDb.connectDatabase()) {
			// TODO: Handle this error
			System.out.println("Failed to connect to MySql database: " + AWS_PASSWORD);
			System.exit(0);
		}

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
			System.out.println("Failed to connect to Sales Force: " + e1.getMessage());
			sqlDb.disconnectDatabase();
			System.exit(0);
		}

		// Get contacts and store in list
		ArrayList<Contact> contactList = getContacts();

		// Update attendance records
		ArrayList<SalesForceAttendanceModel> sfAttendance = pike13Api.getSalesForceAttendance("2018-01-01");
		updateAttendance(sfAttendance, contactList);

		sqlDb.disconnectDatabase();
		System.exit(0);
	}

	private static ArrayList<Contact> getContacts() {
		ArrayList<Contact> contactsList = new ArrayList<Contact>();

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c FROM Contact WHERE Front_Desk_ID__c != null");
			if (queryResults.getSize() > 0) {
				for (int i = 0; i < queryResults.getRecords().length; i++) {
					// Add contact to list
					contactsList.add((Contact) queryResults.getRecords()[i]);
				}
			}

		} catch (Exception e) {
			System.out.println("Failed to get Contacts from Sales Force: " + e.getMessage());
		}

		System.out.println("Added " + contactsList.size() + " contacts to list.");
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
				a.setEvent_Name__c(stripQuotes(inputModel.getEventName()));
				a.setService_Name__c(stripQuotes(inputModel.getServiceName()));
				Calendar cal = Calendar.getInstance();
				String date = stripQuotes(inputModel.getServiceDate());
				cal.set(Calendar.YEAR, Integer.parseInt(date.substring(0, 4)));
				cal.set(Calendar.MONTH, Integer.parseInt(date.substring(5, 7)) - 1);
				cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(date.substring(8, 10)));
				a.setService_Date__c(cal);
				a.setService_TIme__c(stripQuotes(inputModel.getServiceTime()));
				a.setStatus__c(stripQuotes(inputModel.getStatus()));
				a.setSchedule_id__c(inputModel.getScheduleID());
				a.setVisit_Id__c(inputModel.getVisitID());
				a.setLocation__c(stripQuotes(inputModel.getLocation()));
				a.setStaff__c(stripQuotes(inputModel.getStaff()));
				recordList.add(a);
			}

			// Copy up to 200 records to array (.toArray does not seem to work)
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
					System.out.println("Reset remaining records: " + remainingRecords);
				}
			}

			// Update remaining records in Salesforce.com
			if (arrayIdx > 0)
				upsertAttendanceRecords(recordArray);

		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Upsert count: " + upsertCount);
	}

	private static void upsertAttendanceRecords(Student_Attendance__c[] records) {
		UpsertResult[] upsertResults = null;

		try {
			// Update the records in Salesforce.com
			upsertResults = connection.upsert("Visit_Id__c", records);
			upsertCount += records.length;

		} catch (ConnectionException e) {
			System.out.println("Failure with upsert of Sales Force attendance: " + e.getMessage());
		}

		// check the returned results for any errors
		for (int i = 0; i < upsertResults.length; i++) {
			if (!upsertResults[i].isSuccess()) {
				Error[] errors = upsertResults[i].getErrors();
				for (int j = 0; j < errors.length; j++) {
					System.out.println("ERROR updating record: " + errors[j].getMessage());
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
		System.out.println("Failed to find Client ID " + clientID + " in contacts list");
		return null;
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

	private static String stripQuotes(String fieldData) {
		// Strip off quotes around field string
		if (fieldData.equals("\"\"") || fieldData.startsWith("null"))
			return "";
		else
			return fieldData.substring(1, fieldData.length() - 1);
	}
}