package wsc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;

import org.joda.time.DateTime;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.Error;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.UpsertResult;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.soap.enterprise.sobject.Student_Attendance__c;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SalesForceApi {

	static final String USERNAME = "wendy.avis@jointheleague.org";
	static final String PASSWORD = readFile("./sfPassword.txt");
	static EnterpriseConnection connection;

	public static void main(String[] args) {
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(USERNAME);
		config.setPassword(PASSWORD);
		config.setTraceMessage(false);

		try {

			connection = Connector.newConnection(config);

			System.out.println("Auth EndPoint: " + config.getAuthEndpoint());
			System.out.println("Service EndPoint: " + config.getServiceEndpoint());
			System.out.println("Username: " + config.getUsername());
			System.out.println("SessionId: " + config.getSessionId());

			// run the different test cases
			getAttendance();
			updateAttendance();

		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}

		System.exit(0);
	}

	private static void getAttendance() {

		System.out.println("Querying for Attendance...");
		String serviceDate;
		boolean done = false;
		QueryResult queryResult;

		try {
			// query for the newest
			queryResult = connection.query("SELECT Id, Name, Service_Date__c, Service_TIme__c, Event_Name__c,"
					+ "Event_Type__c, Front_Desk_ID__c, Location__c, Location_Code1__c, Service_Name__c, Status__c "
					+ "FROM Student_Attendance__c WHERE Front_Desk_ID__c = '2550878' AND Service_Date__c >= 2018-01-01 "
					+ "ORDER BY Front_Desk_ID__c ASC, Service_Date__c DESC");

			if (queryResult.getSize() > 0) {
				System.out.println("Total of " + queryResult.getSize() + " records.");
				while (!done) {
					SObject[] records = queryResult.getRecords();
					for (int i = 0; i < records.length; i++) {
						// cast the SObject to a strongly-typed class
						Student_Attendance__c a = (Student_Attendance__c) records[i];
						serviceDate = (new DateTime(a.getService_Date__c().getTimeInMillis())).toString("yyyy-MM-dd");
						System.out.println("ClientID: " + a.getFront_Desk_ID__c() + " - Date: " + serviceDate + " "	+ a.getService_TIme__c() 
								+ " - EventType: " + a.getEvent_Type__c() 
								+ " - EventName: " + a.getEvent_Name__c() 
								+ " - Location: " + a.getLocation__c() 
								+ " - LocCode: " + a.getLocation_Code1__c() 
								+ " - ServiceName: " + a.getService_Name__c() 
								+ " - Status: " + a.getStatus__c());
					}
					if (queryResult.isDone())
						done = true;
					else
						queryResult = connection.queryMore(queryResult.getQueryLocator());
				}
			} else
				System.out.println("No records found!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void updateAttendance() {
		System.out.println("\n******** Update attendance ********\n");
		Student_Attendance__c[] records = new Student_Attendance__c[1];

		try {
			Student_Attendance__c a = new Student_Attendance__c();

			a.setContact__r(getContact());
			a.setEvent_Name__c("8@CV Mammoth");
			a.setService_Name__c("[Level 8] @ CV");
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.DAY_OF_MONTH, 14);
			a.setService_Date__c(cal);
			a.setService_TIme__c("17:30");
			a.setStatus__c("Completed");
			a.setSchedule_id__c("68092299");
			a.setVisit_Id__c("114532187");
			a.setSchedule_id__c("68092299");
			a.setLocation__c("Carmel Valley Classroom (Suite #113)");
			a.setStaff__c("Dave Dunn");
			records[0] = a;

			// update the records in Salesforce.com
			UpsertResult[] upsertResults = connection.upsert("Visit_Id__c", records);

			// check the returned results for any errors
			for (int i = 0; i < upsertResults.length; i++) {
				if (upsertResults[i].isSuccess()) {
					System.out.println(i + ". Successfully updated record - Id: " + upsertResults[i].getId());
				} else {
					Error[] errors = upsertResults[i].getErrors();
					for (int j = 0; j < errors.length; j++) {
						System.out.println("ERROR updating record: " + errors[j].getMessage());
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Contact getContact() {
		Contact c = null;

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c " + "FROM Contact WHERE Front_Desk_ID__c = '2550878'");
			if (queryResults.getSize() > 0) {
				c = (Contact) queryResults.getRecords()[0];
				System.out.println("# records: " + queryResults.getSize() + ", " + c.getFront_Desk_Id__c());
			}
			return c;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return c;
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