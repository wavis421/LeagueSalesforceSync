package wsc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.joda.time.DateTime;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import controller.Pike13Api;
import model.AttendanceEventModel;
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
	private static final int PAST_DATE_RANGE_INTERVAL_IN_DAYS = 30;
	private static final int FUTURE_DATE_RANGE_INTERVAL_IN_DAYS = 90;

	private static EnterpriseConnection connection;
	private static MySqlDatabase sqlDb;
	private static Pike13Api pike13Api;
	private static GetRecordsFromSalesForce getRecords;
	private static UpdateRecordsInSalesForce updateRecords;
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
		pike13Api = new Pike13Api(sqlDb, pike13Token);

		// Connect to SalesForce
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(SALES_FORCE_USERNAME);
		config.setPassword(SALES_FORCE_PASSWORD);
		config.setTraceMessage(false);

		try {
			connection = Connector.newConnection(config);

		} catch (ConnectionException e1) {
			e1.printStackTrace();
			exitProgram(LogDataModel.SALES_FORCE_CONNECTION_ERROR, e1.getMessage());
		}

		// Instantiate get & update classes
		getRecords = new GetRecordsFromSalesForce(sqlDb, connection);
		updateRecords = new UpdateRecordsInSalesForce(sqlDb, connection, getRecords);
		ListUtilities.initDatabase(sqlDb);

		// Get SF contacts and accounts and store in list
		ArrayList<Contact> sfContactList = getRecords.getSalesForceContacts();
		ArrayList<Contact> sfAllContactList = getRecords.getAllSalesForceContacts();
		ArrayList<Account> sfAccountList = getRecords.getSalesForceAccounts();

		if (sfContactList != null && sfAllContactList != null && sfAccountList != null) {
			// Get Pike13 clients and upsert to SalesForce
			ArrayList<StudentImportModel> pike13StudentContactList = pike13Api.getClientsForSfImport(false);
			ArrayList<StudentImportModel> pike13AdultContactList = pike13Api.getClientsForSfImport(true);

			// Make sure Pike13 didn't have error getting data
			if (pike13StudentContactList != null && pike13AdultContactList != null) {
				// Insert account ID for all students & adults
				ListUtilities.fillInAccountID(pike13StudentContactList, sfAllContactList);
				ListUtilities.fillInAccountID(pike13AdultContactList, sfAllContactList);

				// Update student & adult contact records
				updateRecords.updateStudents(pike13StudentContactList, pike13AdultContactList, sfAccountList);
				updateRecords.updateAdults(pike13StudentContactList, pike13AdultContactList, sfAccountList);
			}
		}

		// Get Github comments and Pike13 attendance; store in list
		ArrayList<AttendanceEventModel> dbAttendanceList = sqlDb.getAllEvents();
		ArrayList<SalesForceAttendanceModel> sfAttendance = pike13Api.getSalesForceAttendance(startDate, endDate);

		if (sfAttendance != null) {
			// Update attendance records
			updateRecords.updateAttendance(sfAttendance, dbAttendanceList, sfContactList);

			// Delete canceled attendance records
			updateRecords.removeExtraAttendanceRecords(sfAttendance, startDate, endDate);
		}

		// Update Staff Hours records
		ArrayList<StaffMemberModel> sfStaffMembers = pike13Api.getSalesForceStaffMembers();
		ArrayList<SalesForceStaffHoursModel> sfStaffHours = pike13Api.getSalesForceStaffHours(startDate, today);
		if (sfStaffMembers != null && sfStaffHours != null)
			updateRecords.updateStaffHours(sfStaffMembers, sfStaffHours, sfContactList);

		exitProgram(-1, null); // -1 indicates no error
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