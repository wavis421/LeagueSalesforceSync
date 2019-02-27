package controller;

import java.net.HttpURLConnection;
import java.util.ArrayList;

import javax.json.JsonArray;
import javax.json.JsonObject;

import model.SalesForceAttendanceModel;
import model.SalesForceStaffHoursModel;
import model.StaffMemberModel;
import model.StudentImportModel;

public class Pike13SalesforceImport {
	// Custom field names for client data
	private final String GENDER_FIELD = "custom_field_106320";
	private final String GITHUB_FIELD = "custom_field_127885";
	private final String GRAD_YEAR_FIELD = "custom_field_145902";
	private final String SCHOOL_ATTENDING_FIELD = "custom_field_106316";
	private final String TSHIRT_SIZE_FIELD = "custom_field_106318";
	private final String EMERG_CONTACT_NAME_FIELD = "custom_field_106321";
	private final String EMERG_CONTACT_PHONE_FIELD = "custom_field_106322";
	private final String EMERG_CONTACT_EMAIL_FIELD = "custom_field_149434";
	private final String CURRENT_GRADE_FIELD = "custom_field_106463";
	private final String HOME_PHONE_FIELD = "custom_field_106498";
	private final String HEAR_ABOUT_US_FIELD = "custom_field_128371";
	private final String WHO_TO_THANK_FIELD = "custom_field_147039";
	private final String FINANCIAL_AID_FIELD = "custom_field_106317";
	private final String FINANCIAL_AID_PERCENT_FIELD = "custom_field_108413";
	private final String GRANT_INFO_FIELD = "custom_field_148317";
	private final String LEAVE_REASON_FIELD = "custom_field_148655";
	private final String STOP_EMAIL_FIELD = "custom_field_149207";
	private final String CURRENT_LEVEL_FIELD = "custom_field_157737";

	// Custom field names for Staff Member data
	private final String STAFF_SF_CLIENT_ID_FIELD = "custom_field_152501";
	private final String STAFF_CATEGORY_FIELD = "custom_field_106325";
	private final String STAFF_GENDER_FIELD = "custom_field_106320";
	private final String STAFF_HOME_PHONE_FIELD = "custom_field_106498";
	private final String STAFF_OCCUPATION_FIELD = "custom_field_106324";
	private final String STAFF_EMPLOYER_FIELD = "custom_field_133180";
	private final String STAFF_START_INFO_FIELD = "custom_field_140367";
	private final String STAFF_ALTERNATE_EMAIL_FIELD = "custom_field_140368";
	private final String STAFF_KEY_HOLDER_FIELD = "custom_field_149098";
	private final String STAFF_LIVE_SCAN_DATE_FIELD = "custom_field_149097";
	private final String STAFF_T_SHIRT_FIELD = "custom_field_106737";
	private final String STAFF_WHERE_DID_YOU_HEAR_FIELD = "custom_field_128371";
	private final String STAFF_LEAVE_FIELD = "custom_field_149559";
	private final String STAFF_EMERG_NAME_FIELD = "custom_field_106321";
	private final String STAFF_EMERG_EMAIL_FIELD = "custom_field_149434";
	private final String STAFF_EMERG_PHONE_FIELD = "custom_field_106322";
	private final String STAFF_CURR_BOARD_MEMBER_FIELD = "custom_field_153299";
	private final String STAFF_CURR_STAFF_MEMBER_FIELD = "custom_field_153300";
	private final String STAFF_GITHUB_USER_FIELD = "custom_field_127885";
	private final String STAFF_TITLE_FIELD = "custom_field_154726";

	// Indices for client data import to SF
	private final int CLIENT_SF_ID_IDX = 0;
	private final int CLIENT_EMAIL_IDX = 1;
	private final int CLIENT_MOBILE_PHONE_IDX = 2;
	private final int CLIENT_FULL_ADDRESS_IDX = 3;
	private final int CLIENT_BIRTHDATE_IDX = 4;
	private final int CLIENT_COMPLETED_VISITS_IDX = 5;
	private final int CLIENT_FUTURE_VISITS_IDX = 6;
	private final int CLIENT_HAS_SIGNED_WAIVER_IDX = 7;
	private final int CLIENT_HAS_MEMBERSHIP_IDX = 8;
	private final int CLIENT_PASS_ON_FILE_IDX = 9;
	private final int CLIENT_HOME_LOC_LONG_IDX = 10;
	private final int CLIENT_FIRST_NAME_IDX = 11;
	private final int CLIENT_LAST_NAME_IDX = 12;
	private final int CLIENT_SCHOOL_NAME_IDX = 13;
	private final int CLIENT_TSHIRT_SIZE_IDX = 14;
	private final int CLIENT_GENDER_IDX = 15;
	private final int CLIENT_EMERG_CONTACT_NAME_IDX = 16;
	private final int CLIENT_EMERG_CONTACT_PHONE_IDX = 17;
	private final int CLIENT_CURR_GRADE_IDX = 18;
	private final int CLIENT_HEAR_ABOUT_US_IDX = 19;
	private final int CLIENT_GRAD_YEAR_IDX = 20;
	private final int CLIENT_WHO_TO_THANK_IDX = 21;
	private final int CLIENT_EMERG_CONTACT_EMAIL_IDX = 22;
	private final int CLIENT_FINANCIAL_AID_IDX = 23;
	private final int CLIENT_FINANCIAL_AID_PERCENT_IDX = 24;
	private final int CLIENT_GITHUB_IDX = 25;
	private final int CLIENT_GRANT_INFO_IDX = 26;
	private final int CLIENT_LEAVE_REASON_IDX = 27;
	private final int CLIENT_STOP_EMAIL_IDX = 28;
	private final int CLIENT_FIRST_VISIT_IDX = 29;
	private final int CLIENT_HOME_PHONE_IDX = 30;
	private final int CLIENT_ACCOUNT_MGR_NAMES_IDX = 31;
	private final int CLIENT_ACCOUNT_MGR_EMAILS_IDX = 32;
	private final int CLIENT_ACCOUNT_MGR_PHONES_IDX = 33;
	private final int CLIENT_DEPENDENT_NAMES_IDX = 34;
	private final int CLIENT_CURRENT_LEVEL_IDX = 35;
	
	// Indices for SalesForce enrollment data
	private final int SF_PERSON_ID_IDX = 0;
	private final int SF_SERVICE_DATE_IDX = 1;
	private final int SF_SERVICE_TIME_IDX = 2;
	private final int SF_EVENT_NAME_IDX = 3;
	private final int SF_SERVICE_NAME_IDX = 4;
	private final int SF_SERVICE_CATEGORY_IDX = 5;
	private final int SF_STATE_IDX = 6;
	private final int SF_VISIT_ID_IDX = 7;
	private final int SF_EVENT_OCCURRENCE_ID_IDX = 8;
	private final int SF_LOCATION_NAME_IDX = 9;
	private final int SF_INSTRUCTOR_NAMES_IDX = 10;
	private final int SF_FULL_NAME_IDX = 11;

	// Indices for Staff Member data
	private final int TEACHER_CLIENT_ID_IDX = 0;
	private final int TEACHER_FIRST_NAME_IDX = 1;
	private final int TEACHER_LAST_NAME_IDX = 2;
	private final int TEACHER_SF_CLIENT_ID_IDX = 3;
	private final int TEACHER_CATEGORY_IDX = 4;
	private final int TEACHER_ROLE_IDX = 5;
	private final int TEACHER_OCCUPATION_IDX = 6;
	private final int TEACHER_EMPLOYER_IDX = 7;
	private final int TEACHER_START_INFO_IDX = 8;
	private final int TEACHER_GENDER_IDX = 9;
	private final int TEACHER_PHONE_IDX = 10;
	private final int TEACHER_HOME_PHONE_IDX = 11;
	private final int TEACHER_ADDRESS_IDX = 12;
	private final int TEACHER_EMAIL_IDX = 13;
	private final int TEACHER_ALTERNATE_EMAIL_IDX = 14;
	private final int TEACHER_HOME_LOCATION_IDX = 15;
	private final int TEACHER_GITHUB_USER_IDX = 16;
	private final int TEACHER_BIRTHDATE_IDX = 17;
	private final int TEACHER_PAST_EVENTS_IDX = 18;
	private final int TEACHER_FUTURE_EVENTS_IDX = 19;
	private final int TEACHER_KEY_HOLDER_IDX = 20;
	private final int TEACHER_LIVE_SCAN_DATE_IDX = 21;
	private final int TEACHER_T_SHIRT_IDX = 22;
	private final int TEACHER_WHERE_DID_YOU_HEAR_IDX = 23;
	private final int TEACHER_LEAVE_IDX = 24;
	private final int TEACHER_EMERG_NAME_IDX = 25;
	private final int TEACHER_EMERG_EMAIL_IDX = 26;
	private final int TEACHER_EMERG_PHONE_IDX = 27;
	private final int TEACHER_CURR_BOARD_MEMBER_IDX = 28;
	private final int TEACHER_CURR_STAFF_MEMBER_IDX = 29;
	private final int TEACHER_IS_ALSO_CLIENT_IDX = 30;
	private final int TEACHER_TITLE_IDX = 31;

	// Indices for Staff Hours data
	private final int STAFF_CLIENT_ID_IDX = 0;
	private final int STAFF_EVENT_SERVICE_NAME_IDX = 1;
	private final int STAFF_EVENT_SERVICE_DATE_IDX = 2;
	private final int STAFF_EVENT_SERVICE_TIME_IDX = 3;
	private final int STAFF_EVENT_DURATION_IDX = 4;
	private final int STAFF_EVENT_LOCATION_IDX = 5;
	private final int STAFF_EVENT_COMPLETED_COUNT_IDX = 6;
	private final int STAFF_EVENT_NO_SHOW_COUNT_IDX = 7;
	private final int STAFF_EVENT_CANCELED_COUNT_IDX = 8;
	private final int STAFF_EVENT_NAME_IDX = 9;
	private final int STAFF_EVENT_SCHEDULE_ID_IDX = 10;
	private final int STAFF_EVENT_FULL_NAME_IDX = 11;
	private final int STAFF_EVENT_SERVICE_CATEGORY_IDX = 12;

	private final String getClientDataForSF = "{\"data\":{\"type\":\"queries\","
			// Get attributes: fields, page limit and filters
			+ "\"attributes\":{"
			// Select fields for client data import to SF
			+ "\"fields\":[\"person_id\",\"email\",\"phone\",\"address\",\"birthdate\",\"completed_visits\",\"future_visits\","
			+ "            \"has_signed_waiver\",\"has_membership\",\"current_plans\","
			+ "            \"home_location_name\",\"first_name\",\"last_name\",\"" + SCHOOL_ATTENDING_FIELD + "\","
			+ "            \"" + TSHIRT_SIZE_FIELD + "\",\"" + GENDER_FIELD + "\",\"" + EMERG_CONTACT_NAME_FIELD + "\","
			+ "            \"" + EMERG_CONTACT_PHONE_FIELD + "\",\"" + CURRENT_GRADE_FIELD + "\","
			+ "            \"" + HEAR_ABOUT_US_FIELD + "\",\"" + GRAD_YEAR_FIELD + "\",\"" + WHO_TO_THANK_FIELD + "\","
			+ "            \"" + EMERG_CONTACT_EMAIL_FIELD + "\",\"" + FINANCIAL_AID_FIELD + "\",\"" + FINANCIAL_AID_PERCENT_FIELD + "\","
			+ "            \"" + GITHUB_FIELD + "\",\"" + GRANT_INFO_FIELD + "\",\"" + LEAVE_REASON_FIELD + "\","
			+ "            \"" + STOP_EMAIL_FIELD + "\",\"first_visit_date\",\"" + HOME_PHONE_FIELD + "\","
			+ "            \"account_manager_names\",\"account_manager_emails\",\"account_manager_phones\",\"dependent_names\","
			+ "            \"" + CURRENT_LEVEL_FIELD + "\"],"
			// Page limit max is 500
			+ "\"page\":{\"limit\":500";

	private final String getClientDataForSF2student = "},"
			// Filter on Dependents NULL, visited within the last year OR has future visits
			+ "\"filter\":[\"and\",[[\"emp\",\"dependent_names\"],"
			+ "                     [\"eq\",\"person_state\",\"active\"],"
			+ "                     [\"or\",[[\"and\",[[\"emp\",\"days_since_last_visit\"],[\"gt\",\"future_visits\",0]]],"
			+ "                              [\"and\",[[\"nemp\",\"days_since_last_visit\"],[\"lt\",\"days_since_last_visit\",366]]]]]]]}}}";

	private final String getClientDataForSF2adult = "},"
			// Filter on Dependents not NULL
			+ "\"filter\":[\"and\",[[\"nemp\",\"dependent_names\"],"
			+ "                     [\"eq\",\"person_state\",\"active\"]]]}}}";

	private final String getEnrollmentSalesForce = "{\"data\":{\"type\":\"queries\","
			// Get attributes: fields, page limit
			+ "\"attributes\":{"
			// Select fields
			+ "\"fields\":[\"person_id\",\"service_date\",\"service_time\",\"event_name\",\"service_name\","
			+ "            \"service_category\",\"state\",\"visit_id\",\"event_occurrence_id\","
			+ "            \"service_location_name\",\"instructor_names\",\"full_name\"],"
			// Page limit max is 500
			+ "\"page\":{\"limit\":500";

	private final String getEnrollmentSalesForce2 = "},"
			// Filter on between start/end dates OR all future summer slam, workshops and leave
			+ "\"filter\":[\"or\",[[\"btw\",\"service_date\",[\"0000-00-00\",\"1111-11-11\"]],"
			+ "                    [\"and\",[[\"gt\",\"service_date\",\"2222-22-22\"],"
			+ "                              [\"starts\",\"service_category\",\"class jslam\"]]],"
			+ "                    [\"and\",[[\"gt\",\"service_date\",\"2222-22-22\"],"
			+ "                              [\"starts\",\"service_category\",\"works\"]]],"
			+ "                    [\"and\",[[\"gt\",\"service_date\",\"2222-22-22\"],"
			+ "                              [\"eq\",\"service_category\",\"leave\"]]]]]}}}";

	// Get staff member data
	private final String getStaffMemberData = "{\"data\":{\"type\":\"queries\","
			// Get attributes: fields, page limit and filters
			+ "\"attributes\":{"
			// Select fields
			+ "\"fields\":[\"person_id\",\"first_name\",\"last_name\",\"" + STAFF_SF_CLIENT_ID_FIELD + "\","
			+ "            \"" + STAFF_CATEGORY_FIELD + "\",\"role\",\"" + STAFF_OCCUPATION_FIELD + "\","
			+ "            \"" + STAFF_EMPLOYER_FIELD + "\",\"" + STAFF_START_INFO_FIELD + "\","
			+ "            \"" + STAFF_GENDER_FIELD + "\",\"phone\",\"" + STAFF_HOME_PHONE_FIELD + "\",\"address\","
			+ "            \"email\",\"" + STAFF_ALTERNATE_EMAIL_FIELD + "\",\"home_location_name\","
			+ "            \"" + STAFF_GITHUB_USER_FIELD + "\",\"birthdate\",\"past_events\","
			+ "            \"future_events\",\"" + STAFF_KEY_HOLDER_FIELD + "\",\"" + STAFF_LIVE_SCAN_DATE_FIELD + "\","
			+ "            \"" + STAFF_T_SHIRT_FIELD + "\",\"" + STAFF_WHERE_DID_YOU_HEAR_FIELD + "\","
			+ "            \"" + STAFF_LEAVE_FIELD + "\",\"" + STAFF_EMERG_NAME_FIELD + "\",\"" + STAFF_EMERG_EMAIL_FIELD + "\","
			+ "            \"" + STAFF_EMERG_PHONE_FIELD + "\",\"" + STAFF_CURR_BOARD_MEMBER_FIELD + "\","
			+ "            \"" + STAFF_CURR_STAFF_MEMBER_FIELD + "\",\"also_client\",\"" + STAFF_TITLE_FIELD + "\"],"
			// Page limit max is 500
			+ "\"page\":{\"limit\":500},"
			// Filter on Staff Category and staff member active
			+ "\"filter\":[\"and\",[[\"eq\",\"person_state\",\"active\"],"
			+ "                     [\"or\",[[\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Teaching Staff\"],"
			+ "                              [\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Vol Teacher\"],"
			+ "                              [\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Volunteer\"],"
			+ "                              [\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Student TA\"],"
			+ "                              [\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Admin Staff\"],"
			+ "                              [\"eq\",\"" + STAFF_CATEGORY_FIELD + "\",\"Board Member\"]]]]]}}}";

	// Get staff hours data
	private final String getStaffHoursSalesForce = "{\"data\":{\"type\":\"queries\","
			// Get attributes: fields, page limit
			+ "\"attributes\":{"
			// Select fields
			+ "\"fields\":[\"person_id\",\"service_name\",\"service_date\",\"service_time\",\"duration_in_hours\","
			+ "            \"service_location_name\",\"completed_enrollment_count\",\"noshowed_enrollment_count\","
			+ "            \"late_canceled_enrollment_count\",\"event_name\",\"event_occurrence_id\",\"full_name\","
			+ "            \"service_category\"],"
			// Page limit max is 500
			+ "\"page\":{\"limit\":500";

	private final String getStaffHoursSalesForce2 = "},"
			// Filter on since date
			+ "\"filter\":[\"and\",[[\"btw\",\"service_date\",[\"0000-00-00\",\"1111-11-11\"]],"
			+ "                     [\"or\",[[\"eq\",\"attendance_completed\",\"t\"],"
			+ "                              [\"eq\",\"service_name\",\"Volunteer Time\"]]],"
			+ "                     [\"wo\",\"home_location_name\",\"Tax ID#\"]]]}}}";

	Pike13Connect pike13Conn;

	public Pike13SalesforceImport(Pike13Connect pike13Conn) {
		this.pike13Conn = pike13Conn;
	}

	public ArrayList<StudentImportModel> getClientsForSfImport(boolean isAcctMgr) {
		ArrayList<StudentImportModel> studentList = new ArrayList<StudentImportModel>();
		boolean hasMore = false;
		String lastKey = "";

		do {
			// URL connection with authorization
			HttpURLConnection conn;

			// Send the query (set dependents not empty for manager)
			String cmd2;
			if (isAcctMgr)
				cmd2 = getClientDataForSF2adult;
			else
				cmd2 = getClientDataForSF2student;
			if (hasMore)
				conn = pike13Conn.sendQueryToUrl("clients", getClientDataForSF + ",\"starting_after\":\"" + lastKey + "\"" + cmd2);
			else
				conn = pike13Conn.sendQueryToUrl("clients", getClientDataForSF + cmd2);

			// Check result
			if (conn == null)
				return null;

			// Get input stream and read data
			JsonObject jsonObj = pike13Conn.readInputStream(conn);
			if (jsonObj == null) {
				conn.disconnect();
				return null;
			}
			JsonArray jsonArray = jsonObj.getJsonArray("rows");

			for (int i = 0; i < jsonArray.size(); i++) {
				// Get fields for each person
				JsonArray personArray = (JsonArray) jsonArray.get(i);

				// Get fields for this Json array entry
				StudentImportModel model = new StudentImportModel(personArray.getInt(CLIENT_SF_ID_IDX),
						personArray.getString(CLIENT_FIRST_NAME_IDX),
						personArray.getString(CLIENT_LAST_NAME_IDX),
						pike13Conn.stripQuotes(personArray.get(CLIENT_GENDER_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_BIRTHDATE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_CURR_GRADE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_GRAD_YEAR_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_FIRST_VISIT_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_HOME_LOC_LONG_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_EMAIL_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_MOBILE_PHONE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_FULL_ADDRESS_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_SCHOOL_NAME_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_GITHUB_IDX).toString()),
						personArray.getInt(CLIENT_COMPLETED_VISITS_IDX),
						personArray.getInt(CLIENT_FUTURE_VISITS_IDX),
						pike13Conn.stripQuotes(personArray.get(CLIENT_TSHIRT_SIZE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_HAS_SIGNED_WAIVER_IDX).toString()).equals("t") ? true : false,
						pike13Conn.stripQuotes(personArray.get(CLIENT_HAS_MEMBERSHIP_IDX).toString()).equals("t") ? "Yes" : "No",
						pike13Conn.stripQuotes(personArray.get(CLIENT_PASS_ON_FILE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_STOP_EMAIL_IDX).toString()).equals("t") ? true : false,
						pike13Conn.stripQuotes(personArray.get(CLIENT_FINANCIAL_AID_IDX).toString()).equals("t") ? true : false,
						pike13Conn.stripQuotes(personArray.get(CLIENT_FINANCIAL_AID_PERCENT_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_GRANT_INFO_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_LEAVE_REASON_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_HEAR_ABOUT_US_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_WHO_TO_THANK_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_EMERG_CONTACT_NAME_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_EMERG_CONTACT_PHONE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_EMERG_CONTACT_EMAIL_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_HOME_PHONE_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_ACCOUNT_MGR_NAMES_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_ACCOUNT_MGR_PHONES_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_ACCOUNT_MGR_EMAILS_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_DEPENDENT_NAMES_IDX).toString()),
						pike13Conn.stripQuotes(personArray.get(CLIENT_CURRENT_LEVEL_IDX).toString()));

				studentList.add(model);
			}

			// Check to see if there are more pages
			hasMore = jsonObj.getBoolean("has_more");
			if (hasMore)
				lastKey = jsonObj.getString("last_key");

			conn.disconnect();

		} while (hasMore);

		return studentList;
	}

	public ArrayList<SalesForceAttendanceModel> getSalesForceAttendance(String startDate, String endDate) {
		// Get attendance for export to SalesForce database
		ArrayList<SalesForceAttendanceModel> eventList = new ArrayList<SalesForceAttendanceModel>();
		boolean hasMore = false;
		String lastKey = "";

		// Insert start date and end date into enrollment command string
		String enroll2 = getEnrollmentSalesForce2.replaceFirst("0000-00-00", startDate);
		enroll2 = enroll2.replaceFirst("1111-11-11", endDate);
		enroll2 = enroll2.replace("2222-22-22", startDate);

		do {
			// Get URL connection and send the query; add page info if necessary
			HttpURLConnection conn;
			if (hasMore)
				conn = pike13Conn.sendQueryToUrl("enrollments", getEnrollmentSalesForce + ",\"starting_after\":\"" + lastKey + "\"" + enroll2);
			else
				conn = pike13Conn.sendQueryToUrl("enrollments", getEnrollmentSalesForce + enroll2);

			if (conn == null)
				return null;

			// Get input stream and read data
			JsonObject jsonObj = pike13Conn.readInputStream(conn);
			if (jsonObj == null) {
				conn.disconnect();
				return null;
			}
			JsonArray jsonArray = jsonObj.getJsonArray("rows");

			for (int i = 0; i < jsonArray.size(); i++) {
				// Get fields for each event
				JsonArray eventArray = (JsonArray) jsonArray.get(i);

				// Add event to list
				eventList.add(new SalesForceAttendanceModel(eventArray.get(SF_PERSON_ID_IDX).toString(),
						pike13Conn.stripQuotes(eventArray.get(SF_FULL_NAME_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_SERVICE_DATE_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_SERVICE_TIME_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_EVENT_NAME_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_SERVICE_CATEGORY_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_SERVICE_NAME_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_STATE_IDX).toString()),
						eventArray.get(SF_VISIT_ID_IDX).toString(),
						eventArray.get(SF_EVENT_OCCURRENCE_ID_IDX).toString(),
						pike13Conn.stripQuotes(eventArray.get(SF_LOCATION_NAME_IDX).toString()),
						pike13Conn.stripQuotes(eventArray.get(SF_INSTRUCTOR_NAMES_IDX).toString())));
			}

			// Check to see if there are more pages
			hasMore = jsonObj.getBoolean("has_more");
			if (hasMore)
				lastKey = jsonObj.getString("last_key");

			conn.disconnect();

		} while (hasMore);

		return eventList;
	}

	public ArrayList<StaffMemberModel> getSalesForceStaffMembers() {
		ArrayList<StaffMemberModel> staffList = new ArrayList<StaffMemberModel>();

		// Get URL connection and send the query
		HttpURLConnection conn = pike13Conn.sendQueryToUrl("staff_members", getStaffMemberData);
		if (conn == null)
			return null;

		// Get input stream and read data
		JsonObject jsonObj = pike13Conn.readInputStream(conn);
		if (jsonObj == null) {
			conn.disconnect();
			return null;
		}
		JsonArray jsonArray = jsonObj.getJsonArray("rows");

		for (int i = 0; i < jsonArray.size(); i++) {
			// Get fields for each staff member
			JsonArray staffArray = (JsonArray) jsonArray.get(i);

			// Get fields for this Json array entry
			String sfClientID = null;
			if (staffArray.get(TEACHER_SF_CLIENT_ID_IDX) != null)
				sfClientID = pike13Conn.stripQuotes(staffArray.get(TEACHER_SF_CLIENT_ID_IDX).toString());

			staffList.add(new StaffMemberModel(staffArray.get(TEACHER_CLIENT_ID_IDX).toString(), sfClientID,
					staffArray.getString(TEACHER_FIRST_NAME_IDX), staffArray.getString(TEACHER_LAST_NAME_IDX),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_CATEGORY_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_ROLE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_OCCUPATION_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_EMPLOYER_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_START_INFO_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_GENDER_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_BIRTHDATE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_PHONE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_HOME_PHONE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_ADDRESS_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_EMAIL_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_ALTERNATE_EMAIL_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_HOME_LOCATION_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_GITHUB_USER_IDX).toString()),
					staffArray.getInt(TEACHER_PAST_EVENTS_IDX),
					staffArray.getInt(TEACHER_FUTURE_EVENTS_IDX),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_KEY_HOLDER_IDX).toString()).equals("t") ? true : false,
					pike13Conn.stripQuotes(staffArray.get(TEACHER_LIVE_SCAN_DATE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_T_SHIRT_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_WHERE_DID_YOU_HEAR_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_LEAVE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_EMERG_NAME_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_EMERG_EMAIL_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_EMERG_PHONE_IDX).toString()),
					pike13Conn.stripQuotes(staffArray.get(TEACHER_CURR_BOARD_MEMBER_IDX).toString()).equalsIgnoreCase("t") ? true : false,
					pike13Conn.stripQuotes(staffArray.get(TEACHER_CURR_STAFF_MEMBER_IDX).toString()).equalsIgnoreCase("t") ? true : false,
					pike13Conn.stripQuotes(staffArray.get(TEACHER_IS_ALSO_CLIENT_IDX).toString()).equalsIgnoreCase("t") ? true : false,
					pike13Conn.stripQuotes(staffArray.get(TEACHER_TITLE_IDX).toString())));
		}

		conn.disconnect();
		return staffList;
	}

	public ArrayList<SalesForceStaffHoursModel> getSalesForceStaffHours(String startDate, String endDate) {
		// Get staff hours for export to SalesForce database
		ArrayList<SalesForceStaffHoursModel> eventList = new ArrayList<SalesForceStaffHoursModel>();
		boolean hasMore = false;
		String lastKey = "";

		// Insert start date and end date into staff hours command string
		String staffHours2 = getStaffHoursSalesForce2.replaceFirst("0000-00-00", startDate);
		staffHours2 = staffHours2.replaceFirst("1111-11-11", endDate);

		do {
			// Get URL connection and send the query; add page info if necessary
			HttpURLConnection conn;
			if (hasMore)
				conn = pike13Conn.sendQueryToUrl("event_occurrence_staff_members",
						getStaffHoursSalesForce + ",\"starting_after\":\"" + lastKey + "\"" + staffHours2);
			else
				conn = pike13Conn.sendQueryToUrl("event_occurrence_staff_members", getStaffHoursSalesForce + staffHours2);

			if (conn == null)
				return null;

			// Get input stream and read data
			JsonObject jsonObj = pike13Conn.readInputStream(conn);
			if (jsonObj == null) {
				conn.disconnect();
				return null;
			}
			JsonArray jsonArray = jsonObj.getJsonArray("rows");

			for (int i = 0; i < jsonArray.size(); i++) {
				// Get fields for each event
				JsonArray eventArray = (JsonArray) jsonArray.get(i);

				// Add event to list
				eventList.add(new SalesForceStaffHoursModel(eventArray.get(STAFF_CLIENT_ID_IDX).toString(),
						eventArray.getString(STAFF_EVENT_FULL_NAME_IDX),
						eventArray.getString(STAFF_EVENT_SERVICE_NAME_IDX),
						eventArray.getString(STAFF_EVENT_SERVICE_DATE_IDX),
						eventArray.getString(STAFF_EVENT_SERVICE_TIME_IDX),
						eventArray.getJsonNumber(STAFF_EVENT_DURATION_IDX).doubleValue(),
						eventArray.getString(STAFF_EVENT_LOCATION_IDX),
						eventArray.getJsonNumber(STAFF_EVENT_COMPLETED_COUNT_IDX).doubleValue(),
						eventArray.getJsonNumber(STAFF_EVENT_NO_SHOW_COUNT_IDX).doubleValue(),
						eventArray.getJsonNumber(STAFF_EVENT_CANCELED_COUNT_IDX).doubleValue(),
						pike13Conn.stripQuotes(eventArray.get(STAFF_EVENT_NAME_IDX).toString()),
						eventArray.get(STAFF_EVENT_SCHEDULE_ID_IDX).toString(),
						pike13Conn.stripQuotes(eventArray.get(STAFF_EVENT_SERVICE_CATEGORY_IDX).toString())));
			}

			// Check to see if there are more pages
			hasMore = jsonObj.getBoolean("has_more");
			if (hasMore)
				lastKey = jsonObj.getString("last_key");

			conn.disconnect();

		} while (hasMore);

		return eventList;
	}
}
