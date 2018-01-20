package wsc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.joda.time.DateTime;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
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
		config.setTraceMessage(true);

		try {

			connection = Connector.newConnection(config);

			System.out.println("Auth EndPoint: " + config.getAuthEndpoint());
			System.out.println("Service EndPoint: " + config.getServiceEndpoint());
			System.out.println("Username: " + config.getUsername());
			System.out.println("SessionId: " + config.getSessionId());

			// run the different test cases
			queryAttendance();

		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}

		System.exit(0);
	}

	private static void queryAttendance() {

		System.out.println("Querying for Attendance...");
		String serviceDate;
		boolean done = false;
		QueryResult queryResult;

		try {
			// query for the newest
			queryResult = connection.query("SELECT Id, Name, Service_Date__c, Service_TIme__c, Event_Name__c,"
					+ "Event_Type__c, Front_Desk_ID__c, Location__c, Location_Code1__c, Service_Name__c, Status__c "
					+ "FROM Student_Attendance__c WHERE Service_Date__c >= 2018-01-01 "
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