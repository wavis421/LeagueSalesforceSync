package controller;

import java.util.ArrayList;

import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.Contact_Diary__c;

import model.AttendanceEventModel;
import model.LocationLookup;
import model.MySqlDbLogging;
import model.SalesForceAttendanceModel;
import model.StaffMemberModel;
import model.StudentImportModel;
import model.StudentNameModel;

public class ListUtilities {

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
				MySqlDbLogging.insertLogData(errorCode, new StudentNameModel("", "", false), Integer.parseInt(clientID),
						", ClientID " + clientID);
			else if (!eventName.equals(""))
				MySqlDbLogging.insertLogData(errorCode, new StudentNameModel(clientName, "", false),
						Integer.parseInt(clientID), ", " + eventName);
			else
				MySqlDbLogging.insertLogData(errorCode, new StudentNameModel(clientName, "", false),
						Integer.parseInt(clientID), ", ClientID " + clientID + " " + clientName);
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

	public static Contact findStudentContactInPike13List(String studentName, String accountID,
			ArrayList<StudentImportModel> students) {
		for (StudentImportModel s : students) {
			if (s.getAccountID() != null && s.getAccountID().equals(accountID) && s.getFullName().equals(studentName))
				return (Contact) s.getSfContact();
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
			MySqlDbLogging.insertLogData(errorCode, new StudentNameModel(name, "", false), Integer.parseInt(clientID),
					" for " + eventName + " on " + serviceDate);
		}
		return null;
	}

	public static StudentImportModel findAcctManagerInList(StudentImportModel student, String accountMgrName,
			ArrayList<StudentImportModel> mgrList) {
		StudentImportModel partialMatch = null, match = null;

		for (StudentImportModel m : mgrList) {
			String dependents = m.getDependentNames().toLowerCase();

			if (accountMgrName.equalsIgnoreCase(m.getFullName())
					&& dependents.contains(student.getFullName().toLowerCase())) {
				if (m.getAccountID() == null || m.getAccountID().equals(student.getAccountID())) {
					return m;
					
				} else if (student.getAccountID() == null) {
					student.setAccountID(m.getAccountID());
					match = m;
					
				} else {
					System.out.println("Partial Acct Mgr match: " + student.getFullName() + ", " + student.getAccountID() + ", " + student.getAccountMgrNames() + ", " 
							+ accountMgrName + ", " + m.getAccountID() + ", " + m.getDependentNames() + ", " + student.getClientID());
					partialMatch = m;
				}
			}
		}

		// Since there was no better match in the list, use the partial match
		if (match != null)
			return match;
		
		else if (partialMatch != null) {
			return partialMatch;
		}

		return null;
	}

	public static Account findAccountNameInList(String studentName, String adultAccountID, ArrayList<StudentImportModel> studentList,
			ArrayList<StudentImportModel> adultList, ArrayList<Account> sfAcctList) {
		// Find matching student, then get Account using account manager name
		for (StudentImportModel s : studentList) {
			if (studentName.equalsIgnoreCase(s.getFullName()) && (adultAccountID == null || adultAccountID.equals(s.getAccountID()))) {
				String accountMgrName = getFirstNameInString(s.getAccountMgrNames());
				if (!accountMgrName.equals("")) {
					StudentImportModel acctMgrModel = findAcctManagerInList(s, accountMgrName, adultList);
					if (acctMgrModel != null && (adultAccountID == null || acctMgrModel.getAccountID().equals(adultAccountID))) {
						String acctName = acctMgrModel.getLastName() + " " + acctMgrModel.getFirstName() + " Family";
						return findAccountInSalesForceList(acctName, acctMgrModel, sfAcctList);
					} else {
						System.out.println("Find Account fail: " + adultAccountID + ", " + acctMgrModel.getAccountID() + studentName);
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
				else {
					System.out.println("Partial SF Acct match: " + accountMgrName + ", " + a.getId());
					partialMatch = a;
				}
			}
		}

		// Since there was no better match in the list, use the partial match
		if (partialMatch != null) {
			//return partialMatch;
		}

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
			if (c != null) {
				m.setAccountID(c.getAccountId());
				m.setSfContact(c);
			}
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

		for (int i = 1; i <= LocationLookup.getNumLocactions(); i++) {
			String locCodeString = LocationLookup.getLocationCodeString(i);

			locPos = sourceString.indexOf("@ " + locCodeString + " ");
			if (locPos >= 0) {
				String locSubString = sourceString.substring(locPos + 2);
				return locSubString.substring(0, locSubString.indexOf(" "));
			}

			locPos = sourceString.indexOf("@" + locCodeString + " ");
			if (locPos >= 0) {
				String locSubString = sourceString.substring(locPos + 1);
				return locSubString.substring(0, locSubString.indexOf(" "));
			}
		}
		return null;
	}

	public static String findDiaryIdInList(String clientLevelKey, ArrayList<Contact_Diary__c> diaryList) {
		for (Contact_Diary__c d : diaryList) {
			if (clientLevelKey.equals(d.getPike_13_ID_Level__c())) {
				return d.getId();
			}
		}
		return null;
	}
}
