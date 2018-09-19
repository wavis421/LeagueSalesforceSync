package controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import model.LocationLookup;
import model.LogDataModel;
import model.MySqlDatabase;
import model.MySqlDbLogging;
import model.StudentNameModel;

public class SalesForceApi {

	private static final String SALES_FORCE_USERNAME = "leaguebot@jointheleague.org";
	private static final String SALES_FORCE_PASSWORD = readFile("./sfPassword.txt");
	private static final String AWS_PASSWORD = readFile("./awsPassword.txt");
	private static final int DATE_RANGE_PAST_IN_DAYS = 21;
	private static final int DATE_RANGE_FUTURE_IN_DAYS = 45;
	private static final int DATE_RANGE_ENROLL_STATS_DAYS = 20;

	private static MySqlDatabase sqlDb;
	private static Pike13Api pike13Api;
	private static EnterpriseConnection salesForceApi;

	private static String startDate;
	private static String endDate;
	private static String today;

	public static void main(String[] args) {
		// Connect to MySql database for logging status/errors
		sqlDb = new MySqlDatabase(AWS_PASSWORD, MySqlDatabase.SALES_FORCE_SYNC_SSH_PORT);
		if (!sqlDb.connectDatabase()) {
			// TODO: Handle this error
			System.out.println("Failed to connect to MySql database");
			System.exit(0);
		}

		// Set start/end date to +/- 30 days
		DateTime t = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));
		today = t.toString("yyyy-MM-dd");
		startDate = t.minusDays(DATE_RANGE_PAST_IN_DAYS).toString("yyyy-MM-dd");
		endDate = t.plusDays(DATE_RANGE_FUTURE_IN_DAYS).toString("yyyy-MM-dd");

		new MySqlDbLogging(sqlDb);
		MySqlDbLogging.insertLogData(LogDataModel.STARTING_SALES_FORCE_IMPORT, new StudentNameModel("", "", false), 0,
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
			salesForceApi = Connector.newConnection(config);

		} catch (ConnectionException e1) {
			e1.printStackTrace();
			exitProgram(LogDataModel.SALES_FORCE_CONNECTION_ERROR, e1.getMessage());
		}

		// Perform the update to SalesForce
		SalesForceImportEngine importer = new SalesForceImportEngine(sqlDb, pike13Api, salesForceApi);
		LocationLookup.setLocationData(sqlDb.getLocationList());
		importer.updateSalesForce(today, startDate, endDate, DATE_RANGE_ENROLL_STATS_DAYS);

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
			MySqlDbLogging.insertLogData(LogDataModel.SALES_FORCE_IMPORT_COMPLETE, new StudentNameModel("", "", false),
					0, " from " + startDate + " to " + endDate + " ***");
		} else {
			// Failure
			MySqlDbLogging.insertLogData(errorCode, new StudentNameModel("", "", false), 0, ": " + errorMessage);
			MySqlDbLogging.insertLogData(LogDataModel.SALES_FORCE_IMPORT_ABORTED, new StudentNameModel("", "", false),
					0, " from " + startDate + " to " + endDate + " ***");
		}
		sqlDb.disconnectDatabase();
		System.exit(0);
	}
}