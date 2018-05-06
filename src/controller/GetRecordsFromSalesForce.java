package controller;

import java.util.ArrayList;

import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;

import model.LogDataModel;
import model.MySqlDatabase;
import model.StudentNameModel;

public class GetRecordsFromSalesForce {
	private MySqlDatabase sqlDb;
	private EnterpriseConnection connection;

	public GetRecordsFromSalesForce(MySqlDatabase sqlDb, EnterpriseConnection connection) {
		this.sqlDb = sqlDb;
		this.connection = connection;
	}

	public ArrayList<Contact> getSalesForceContacts() {
		ArrayList<Contact> contactsList = new ArrayList<Contact>();

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c FROM Contact WHERE Front_Desk_ID__c != null "
							+ "AND (Record_Type__c = 'Student' OR Staff_Category__c = 'Vol Teacher' "
							+ "OR Staff_Category__c = 'Teaching Staff' OR Staff_Category__c = 'Student TA' "
							+ "OR Staff_Category__c = 'Volunteer' OR Staff_Category__c = 'Admin Staff')");

			if (queryResults.getSize() > 0) {
				for (int i = 0; i < queryResults.getRecords().length; i++) {
					// Add contact to list
					contactsList.add((Contact) queryResults.getRecords()[i]);
				}
			}

		} catch (Exception e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_CONTACTS_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
			return null;
		}

		return contactsList;
	}

	public ArrayList<Contact> getAllSalesForceContacts() {
		ArrayList<Contact> contactsList = new ArrayList<Contact>();
		int recordsProcessed = 0;

		try {
			QueryResult queryResults = connection
					.query("SELECT Front_Desk_ID__c, AccountId, Contact_Type__c, FirstName, LastName, Last_Class_Level__c "
							+ "FROM Contact WHERE Front_Desk_ID__c != null");

			while (queryResults.getSize() > recordsProcessed) {
				recordsProcessed += queryResults.getRecords().length;

				// Get up to 2000 records at one time
				for (int i = 0; i < queryResults.getRecords().length; i++) {
					// Add contact to list if Front Desk ID is numeric
					Contact c = (Contact) queryResults.getRecords()[i];
					contactsList.add(c);
				}

				if (queryResults.getSize() > recordsProcessed) {
					// Still more records to get
					String queryLocator = queryResults.getQueryLocator();
					queryResults = connection.queryMore(queryLocator);
				}
			}

		} catch (Exception e) {
			sqlDb.insertLogData(LogDataModel.SALES_FORCE_CONTACTS_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
			return null;
		}

		return contactsList;
	}

	public ArrayList<Account> getSalesForceAccounts() {
		ArrayList<Account> accountList = new ArrayList<Account>();

		try {
			QueryResult queryResults = connection
					.query("SELECT Id, Name FROM Account WHERE IsDeleted = false AND Type = 'Family'");

			if (queryResults.getSize() > 0) {
				for (int i = 0; i < queryResults.getRecords().length; i++) {
					// Add account to list
					accountList.add((Account) queryResults.getRecords()[i]);
				}
			}

		} catch (Exception e) {
			if (e.getMessage() == null) {
				e.printStackTrace();
				sqlDb.insertLogData(LogDataModel.SF_ACCOUNT_IMPORT_ERROR, new StudentNameModel("", "", false), 0, "");

			} else
				sqlDb.insertLogData(LogDataModel.SF_ACCOUNT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
			return null;
		}

		return accountList;
	}

	public Account getSalesForceAccountByName(String accountMgrName) {
		Account account = null;
		String name = accountMgrName.replace("'", "\\'");

		try {
			QueryResult queryResults = connection
					.query("SELECT Id, Name, Client_id__c FROM Account WHERE Name = '" + name + "'");

			if (queryResults.getSize() > 0) {
				account = (Account) queryResults.getRecords()[0];
				if (queryResults.getSize() > 1)
					sqlDb.insertLogData(LogDataModel.DUPLICATE_SF_ACCOUNT_NAME, new StudentNameModel("", "", false), 0,
							" '" + accountMgrName + "'");
			} else {
				account = new Account();
				account.setName("");
			}

		} catch (Exception e) {
			if (e.getMessage() == null) {
				sqlDb.insertLogData(LogDataModel.SF_ACCOUNT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						" for " + accountMgrName);
				e.printStackTrace();
			} else
				sqlDb.insertLogData(LogDataModel.SF_ACCOUNT_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
						" for " + accountMgrName + ": " + e.getMessage());
		}

		return account;
	}
}