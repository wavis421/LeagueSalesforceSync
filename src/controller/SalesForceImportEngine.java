package controller;

import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.Contact_Diary__c;

import model.AttendanceEventModel;
import model.GraduationModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.SalesForceEnrollStatsModel;
import model.SalesForceStaffHoursModel;
import model.StaffMemberModel;
import model.StudentImportModel;

public class SalesForceImportEngine {

	private MySqlDatabase sqlDb;
	private Pike13Api pike13Api;
	private EnterpriseConnection salesForceApi;

	public SalesForceImportEngine(MySqlDatabase sqlDb, Pike13Api pike13Api, EnterpriseConnection salesForceApi) {
		this.sqlDb = sqlDb;
		this.pike13Api = pike13Api;
		this.salesForceApi = salesForceApi;
	}

	public void updateSalesForce(String today, String startDate, String endDate, int enrollCountDays) {
		// Instantiate get & update classes
		GetRecordsFromSalesForce getRecords = new GetRecordsFromSalesForce(salesForceApi);
		UpdateRecordsInSalesForce updateRecords = new UpdateRecordsInSalesForce(sqlDb, salesForceApi, getRecords);

		// Get SF contacts and accounts and store in list
		ArrayList<Contact> sfContactList = getRecords.getSalesForceContacts(); // Students & teachers
		ArrayList<Contact> sfAllContactList = getRecords.getAllSalesForceContacts(); // + all adults
		ArrayList<Account> sfAccountList = getRecords.getSalesForceAccounts();

		ArrayList<StudentImportModel> pike13StudentContactList = null;
		ArrayList<StudentImportModel> pike13AdultContactList = null;

		// === UPDATE CLIENTS: Students & Parents ===
		if (sfAllContactList != null && sfAccountList != null) {
			// Get Pike13 clients and upsert to SalesForce
			pike13StudentContactList = pike13Api.getClientsForSfImport(false);
			pike13AdultContactList = pike13Api.getClientsForSfImport(true);

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

		// === UPDATE ATTENDANCE ===
		// (1) Get Github comments and Pike13 attendance; store in list
		ArrayList<AttendanceEventModel> dbAttendanceList = sqlDb.getAllEvents(startDate);
		ArrayList<SalesForceAttendanceModel> pike13Attendance = pike13Api.getSalesForceAttendance(startDate, endDate);
		ArrayList<StaffMemberModel> pike13StaffMembers = pike13Api.getSalesForceStaffMembers();

		// (2) Get SF contacts again in case new students were added
		if (sfContactList != null && sfAllContactList != null) {
			sfContactList.clear();
			sfAllContactList.clear();
			sfContactList = getRecords.getSalesForceContacts(); // Students & teachers
			sfAllContactList = getRecords.getAllSalesForceContacts(); // + all adults
		}

		if (pike13Attendance != null && dbAttendanceList != null && sfContactList != null && sfAllContactList != null
				&& pike13StudentContactList != null && pike13StaffMembers != null) {
			// (3) Update attendance records
			updateRecords.updateAttendance(pike13Attendance, dbAttendanceList, sfContactList, sfAllContactList,
					pike13StudentContactList, pike13StaffMembers);

			// (4) Delete canceled attendance records
			updateRecords.removeExtraAttendanceRecords(pike13Attendance, startDate, endDate, pike13StudentContactList);
		}

		// === UPDATE ENROLLMENT STATISTICS for yesterday ===
		DateTime t = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));
		String statsStart = t.minusDays(enrollCountDays + 2).toString("yyyy-MM-dd");
		String statsEnd = t.plusDays(enrollCountDays - 1).toString("yyyy-MM-dd");
		ArrayList<SalesForceEnrollStatsModel> pike13EnrollStats = pike13Api.getSalesForceEnrollStats(statsStart,
				statsEnd);

		if (pike13EnrollStats != null && sfContactList != null) {
			updateRecords.updateEnrollStats(pike13EnrollStats, t.minusDays(1), statsStart, statsEnd,
					pike13StudentContactList, sfContactList);
		}

		// === UPDATE GRADUATION DIARY ENTRIES ===
		ArrayList<Contact_Diary__c> sfDiaryList = getRecords.getSalesForceDiary();
		ArrayList<GraduationModel> gradList = sqlDb.getAllGradRecords();

		if (gradList != null && gradList.size() > 0) {
			// Update records, then remove any processed records
			updateRecords.updateGraduates(gradList, sfContactList, sfDiaryList);
			sqlDb.removeProcessedGraduations();
		}

		// === UPDATE STAFF MEMBERS AND HOURS ===
		ArrayList<SalesForceStaffHoursModel> pike13StaffHours = pike13Api.getSalesForceStaffHours(startDate, today);

		if (pike13StaffMembers != null && pike13StaffHours != null && sfAllContactList != null && sfAccountList != null
				&& sfContactList != null) {
			// Insert account ID into staff records
			ListUtilities.fillInAccountIDForStaff(pike13StaffMembers, sfAllContactList);

			// Update staff member data and hours
			updateRecords.updateStaffMembers(pike13StaffMembers, sfAllContactList, sfAccountList);
			updateRecords.updateStaffHours(pike13StaffMembers, pike13StaffHours, sfContactList);
		}
	}
}
