package controller;

import java.util.ArrayList;

import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;

import model.AttendanceEventModel;
import model.LocationModel;
import model.MySqlDatabase;
import model.SalesForceAttendanceModel;
import model.StaffMemberModel;
import model.StudentImportModel;
import model.StudentNameModel;

public class ListUtilities {
	private static MySqlDatabase sqlDb;

	public static void initDatabase(MySqlDatabase db) {
		sqlDb = db;
	}

	public static Contact findClientIDInList(int errorCode, String clientID, String clientName, String eventName,
			ArrayList<Contact> contactList) {
		for (Contact c : contactList) {
			if (c.getFront_Desk_Id__c().equals(clientID)) {
				return c;
			}
		}

		// -1 indicates error not to be posted
		if (errorCode >= 0) {
			if (clientName == null || clientName.startsWith("null"))
				sqlDb.insertLogData(errorCode, new StudentNameModel("", "", false), Integer.parseInt(clientID),
						", ClientID " + clientID);
			else if (!eventName.equals(""))
				sqlDb.insertLogData(errorCode, new StudentNameModel(clientName, "", false), Integer.parseInt(clientID),
						", " + eventName);
			else
				sqlDb.insertLogData(errorCode, new StudentNameModel(clientName, "", false), Integer.parseInt(clientID),
						", ClientID " + clientID + " " + clientName);
		}
		return null;
	}
	
	public static StaffMemberModel findStaffNameInList(String clientName, ArrayList<StaffMemberModel> staffList) {
		for (StaffMemberModel s : staffList) {
			if (clientName.equals(s.getFullName())) {
				return s;
			}
		}
		return null;
	}

	public static StudentImportModel findClientIDInPike13List(String clientIDString,
			ArrayList<StudentImportModel> clientList) {
		if (!clientIDString.matches("\\d+") || clientList == null)
			return null;

		int clientID = Integer.parseInt(clientIDString);

		for (StudentImportModel m : clientList) {
			if (m.getClientID() == clientID)
				return m;
		}
		return null;
	}

	public static boolean findVisitIdInList(String visitID, ArrayList<SalesForceAttendanceModel> attendanceList) {
		for (SalesForceAttendanceModel a : attendanceList) {
			if (a.getVisitID().equals(visitID)) {
				return true;
			}
		}
		return false;
	}

	public static AttendanceEventModel findAttendanceEventInList(String visitID,
			ArrayList<AttendanceEventModel> attendList) {
		if (visitID == null || visitID.equals("") || visitID.equals("0"))
			return null;

		for (AttendanceEventModel a : attendList) {
			if (a.getVisitID() != 0 && String.valueOf(a.getVisitID()).equals(visitID))
				return a;
		}
		return null;
	}

	public static StaffMemberModel findStaffIDInList(int errorCode, String clientID, String name, String serviceDate,
			String eventName, ArrayList<StaffMemberModel> staffList) {
		for (StaffMemberModel s : staffList) {
			if (s.getClientID().equals(clientID)) {
				return s;
			}
		}

		if (errorCode != -1) {
			// Truncate service name up to '@' character
			if (eventName != null) {
				int pos = eventName.indexOf('(');
				if (pos > 0)
					eventName = eventName.substring(0, pos).trim();
			}
			sqlDb.insertLogData(errorCode, new StudentNameModel(name, "", false), Integer.parseInt(clientID),
					" for " + eventName + " on " + serviceDate);
		}
		return null;
	}

	public static StudentImportModel findAcctManagerInList(StudentImportModel student, String accountMgrName,
			ArrayList<StudentImportModel> mgrList) {
		StudentImportModel partialMatch = null;

		for (StudentImportModel m : mgrList) {
			String dependents = m.getDependentNames().toLowerCase();

			if (accountMgrName.equalsIgnoreCase(m.getFullName())
					&& dependents.contains(student.getFullName().toLowerCase())) {
				if (m.getAccountID() == null || m.getAccountID().equals(student.getAccountID())) {
					return m;
				} else {
					partialMatch = m;
				}
			}
		}

		// Since there was no better match in the list, use the partial match
		if (partialMatch != null)
			return partialMatch;

		return null;
	}

	public static Account findAccountNameInList(String studentName, ArrayList<StudentImportModel> studentList,
			ArrayList<StudentImportModel> adultList, ArrayList<Account> sfAcctList) {
		// Find matching student, then get Account using account manager name
		for (StudentImportModel s : studentList) {
			if (studentName.equalsIgnoreCase(s.getFullName())) {
				String accountMgrName = getFirstNameInString(s.getAccountMgrNames());
				if (!accountMgrName.equals("")) {
					StudentImportModel acctMgrModel = findAcctManagerInList(s, accountMgrName, adultList);
					if (acctMgrModel != null) {
						String acctName = acctMgrModel.getLastName() + " " + acctMgrModel.getFirstName() + " Family";
						return findAccountInSalesForceList(acctName, acctMgrModel, sfAcctList);
					}
				}
			}
		}

		// Account not found, so create account with empty name
		Account a = new Account();
		a.setName("");
		return a;
	}

	public static Account findAccountInSalesForceList(String accountMgrName, StudentImportModel accountMgrModel,
			ArrayList<Account> acctList) {

		Account partialMatch = null;

		for (Account a : acctList) {
			if (accountMgrName.equalsIgnoreCase(a.getName())) {
				if (accountMgrModel.getAccountID() == null || accountMgrModel.getAccountID().equals(a.getId()))
					return a;
				else
					partialMatch = a;
			}
		}

		// Since there was no better match in the list, use the partial match
		if (partialMatch != null)
			return partialMatch;

		// Account not in list, so create new account with empty name
		Account account = new Account();
		account.setName("");
		return account;
	}

	public static Account findAccountByName(String familyName, ArrayList<Account> acctList) {
		for (Account a : acctList) {
			if (familyName.equalsIgnoreCase(a.getName())) {
				return a;
			}
		}
		return null;
	}

	public static String getFirstNameInString(String nameString) {
		String name1 = nameString.trim();
		int idx = nameString.indexOf(',');
		if (idx > 0)
			name1 = nameString.substring(0, idx);

		return name1;
	}

	public static void fillInAccountID(ArrayList<StudentImportModel> clientList, ArrayList<Contact> contacts) {
		for (StudentImportModel m : clientList) {
			Contact c = ListUtilities.findClientIDInList(-1, String.valueOf(m.getClientID()), m.getFullName(), "",
					contacts);
			if (c != null)
				m.setAccountID(c.getAccountId());
		}
	}

	public static void fillInAccountIDForStaff(ArrayList<StaffMemberModel> clientList, ArrayList<Contact> contacts) {
		for (StaffMemberModel m : clientList) {
			Contact c = ListUtilities.findClientIDInList(-1, String.valueOf(m.getClientID()), m.getFullName(), "",
					contacts);
			if (c != null)
				m.setAccountID(c.getAccountId());
		}
	}

	public static String findLocCodeInList(String sourceString) {
		int locPos;

		for (int i = 0; i < LocationModel.getNumLocactions(); i++) {
			locPos = sourceString.indexOf("@ " + LocationModel.getLocationCodeString(i));
			if (locPos >= 0 && sourceString.length() > (locPos + 4))
				return sourceString.substring(locPos + 2, locPos + 4);

			locPos = sourceString.indexOf("@" + LocationModel.getLocationCodeString(i));
			if (locPos >= 0 && sourceString.length() > (locPos + 3))
				return sourceString.substring(locPos + 1, locPos + 3);
		}
		return null;
	}
}
