package model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;

/**
 * MySqlDbImports
 * 
 * @author wavis
 *
 *         This file takes data from Pike13 and updates the Student Tracker AWS
 *         Database.
 * 
 */

public class MySqlDbImports {
	private static final int CLASS_NAME_WIDTH = 40;
	private static final int COMMENT_WIDTH = 150;
	private static final int REPO_NAME_WIDTH = 50;

	private MySqlDatabase sqlDb;

	public MySqlDbImports(MySqlDatabase sqlDb) {
		this.sqlDb = sqlDb;
	}

	/*
	 * ------- Student Import Database Queries -------
	 */
	public ArrayList<StudentModel> getActiveStudents() {
		ArrayList<StudentModel> nameList = new ArrayList<StudentModel>();
		DateTime today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));

		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT * FROM Students WHERE isInMasterDb ORDER BY FirstName, LastName;");
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					nameList.add(new StudentModel(result.getInt("ClientID"),
							new StudentNameModel(result.getString("FirstName"), result.getString("LastName"),
									result.getBoolean("isInMasterDb")),
							sqlDb.getAge(today, result.getString("Birthdate")), result.getString("GithubName"),
							result.getInt("Gender"), result.getDate("StartDate"), result.getInt("Location"),
							result.getInt("GradYear"), result.getString("CurrentClass"), result.getString("Email"),
							result.getString("AcctMgrEmail"), result.getString("EmergencyEmail"),
							result.getString("Phone"), result.getString("AcctMgrPhone"), result.getString("HomePhone"),
							result.getString("EmergencyPhone"), result.getString("CurrentModule"),
							result.getString("CurrentLevel"), result.getString("RegisterClass"),
							result.getDate("LastVisitDate")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getActiveStudents: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return nameList;
	}

	public void importStudents(ArrayList<StudentImportModel> importList) {
		ArrayList<StudentImportModel> dbList = getAllStudentsAsImportData();
		int dbListIdx = 0;
		int dbListSize = dbList.size();

		StudentImportModel dbStudent;
		for (int i = 0; i < importList.size(); i++) {
			StudentImportModel importStudent = importList.get(i);

			// Log any missing data
			logMissingStudentData(importStudent);

			// If at end of DB list, then default operation is insert (1)
			int compare = 1;
			if (dbListIdx < dbListSize) {
				dbStudent = dbList.get(dbListIdx);
				compare = dbStudent.compareTo(importStudent);
			}

			if (compare == 0) {
				// ClientID and all data matches
				checkMissingLevel(dbList.get(dbListIdx), importStudent);
				dbListIdx++;
				continue;

			} else if (compare == -1) {
				// Extra clientID in database
				while (dbListIdx < dbListSize && dbList.get(dbListIdx).getClientID() < importStudent.getClientID()) {
					// Mark student as not in master DB
					if (dbList.get(dbListIdx).getIsInMasterDb() == 1)
						updateIsInMasterDb(dbList.get(dbListIdx), 0);
					dbListIdx++;
				}
				if (dbListIdx < dbListSize) {
					if (dbList.get(dbListIdx).getClientID() == importStudent.getClientID()) {
						// Now that clientID's match, compare and update again
						checkMissingLevel(dbList.get(dbListIdx), importStudent);
						if (dbList.get(dbListIdx).compareTo(importStudent) != 0) {
							updateStudent(importStudent, dbList.get(dbListIdx));
						}
						dbListIdx++;
					} else {
						// Import student is new, insert into DB
						insertStudent(importStudent);
					}
				} else {
					// Import student is new, insert into DB
					insertStudent(importStudent);
				}

			} else if (compare == 1) {
				// Insert new student into DB
				insertStudent(importStudent);

			} else {
				// ClientID matches but data has changed
				checkMissingLevel(dbList.get(dbListIdx), importStudent);
				updateStudent(importStudent, dbList.get(dbListIdx));
				dbListIdx++;
			}
		}
	}

	private void checkMissingLevel(StudentImportModel dbStudent, StudentImportModel importStudent) {
		if (importStudent.getCurrLevel().equals("")
				&& (isJavaClass(dbStudent.getCurrClass()) || isJavaClass(dbStudent.getRegClass()))) {
			String currLevel = "";
			if (!dbStudent.getCurrLevel().equals(""))
				currLevel = " (student level " + dbStudent.getCurrLevel() + ")";
			MySqlDbLogging.insertLogData(LogDataModel.MISSING_CURRENT_LEVEL,
					new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
					importStudent.getClientID(), currLevel);
		}
	}

	private boolean isJavaClass(String className) {
		if (className.startsWith("AD@") || className.startsWith("AG@") || className.startsWith("PG@")
				|| className.startsWith("Java@") || className.startsWith("EL@"))
			return true;
		else
			return false;
	}

	private void logMissingStudentData(StudentImportModel importStudent) {
		if (importStudent.getIsInMasterDb() == 1 && importStudent.getBirthDate().equals(""))
			MySqlDbLogging.insertLogData(LogDataModel.MISSING_BIRTHDATE,
					new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
					importStudent.getClientID(), "");

		if (importStudent.getGradYear() == 0)
			MySqlDbLogging.insertLogData(LogDataModel.MISSING_GRAD_YEAR,
					new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
					importStudent.getClientID(), "");

		if (importStudent.getHomeLocation() == 0) {
			if (importStudent.getHomeLocAsString().equals(""))
				MySqlDbLogging.insertLogData(LogDataModel.MISSING_HOME_LOCATION,
						new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
						importStudent.getClientID(), "");
			else
				MySqlDbLogging.insertLogData(LogDataModel.UNKNOWN_HOME_LOCATION,
						new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
						importStudent.getClientID(), " (" + importStudent.getHomeLocAsString() + ")");
		}

		if (importStudent.getGender() == GenderModel.getGenderUnknown())
			MySqlDbLogging.insertLogData(LogDataModel.MISSING_GENDER,
					new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
					importStudent.getClientID(), "");
	}

	private ArrayList<StudentImportModel> getAllStudentsAsImportData() {
		ArrayList<StudentImportModel> nameList = new ArrayList<StudentImportModel>();

		// Convert student data to import data format
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT * FROM Students ORDER BY ClientID;");
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					String startDateString;
					if (result.getDate("StartDate") == null)
						startDateString = "";
					else
						startDateString = result.getDate("StartDate").toString();

					nameList.add(new StudentImportModel(result.getInt("ClientID"), result.getString("LastName"),
							result.getString("FirstName"), result.getString("GithubName"), result.getInt("Gender"),
							startDateString, result.getInt("Location"), result.getInt("GradYear"),
							result.getInt("isInMasterDb"), result.getString("Email"), result.getString("AcctMgrEmail"),
							result.getString("EmergencyEmail"), result.getString("Phone"),
							result.getString("AcctMgrPhone"), result.getString("HomePhone"),
							result.getString("EmergencyPhone"), result.getString("Birthdate"),
							result.getString("TASinceDate"), result.getInt("TAPastEvents"),
							result.getString("CurrentLevel"), result.getString("CurrentClass"),
							result.getString("LastScore"), result.getString("RegisterClass")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getAllStudentsAsImportData: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return nameList;
	}

	private void insertStudent(StudentImportModel student) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement addStudentStmt = sqlDb.dbConnection.prepareStatement(
						"INSERT INTO Students (ClientID, LastName, FirstName, GithubName, NewGithub, NewStudent, "
								+ "Gender, StartDate, Location, GradYear, isInMasterDb, Email, EmergencyEmail, "
								+ "AcctMgrEmail, Phone, AcctMgrPhone, HomePhone, EmergencyPhone, Birthdate, "
								+ "TASinceDate, TAPastEvents, CurrentLevel) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

				int col = 1;
				addStudentStmt.setInt(col++, student.getClientID());
				addStudentStmt.setString(col++, student.getLastName());
				addStudentStmt.setString(col++, student.getFirstName());
				if (student.getGithubName().equals(""))
					addStudentStmt.setString(col++, null);
				else
					addStudentStmt.setString(col++, student.getGithubName());
				addStudentStmt.setInt(col++, 0);
				addStudentStmt.setInt(col++, 1);
				addStudentStmt.setInt(col++, student.getGender());
				if (!student.getStartDate().equals(""))
					addStudentStmt.setDate(col++, java.sql.Date.valueOf(student.getStartDate()));
				else
					addStudentStmt.setDate(col++, null);
				addStudentStmt.setInt(col++, student.getHomeLocation());
				addStudentStmt.setInt(col++, student.getGradYear());
				addStudentStmt.setString(col++, student.getEmail());
				addStudentStmt.setString(col++, student.getEmergContactEmail());
				addStudentStmt.setString(col++, student.getAccountMgrEmails());
				addStudentStmt.setString(col++, student.getPhone1());
				addStudentStmt.setString(col++, student.getAccountMgrPhones());
				addStudentStmt.setString(col++, student.getPhone2());
				addStudentStmt.setString(col++, student.getEmergContactPhone());
				addStudentStmt.setString(col++, student.getBirthDate());
				addStudentStmt.setString(col++, student.getStaffSinceDate());
				addStudentStmt.setInt(col++, student.getStaffPastEvents());
				addStudentStmt.setString(col++, student.getCurrLevel());

				addStudentStmt.executeUpdate();
				addStudentStmt.close();

				if (student.getGithubName() == null)
					System.out.println("Added new student: " + student.getFirstName() + " " + student.getLastName());
//					MySqlDbLogging.insertLogData(LogDataModel.ADD_NEW_STUDENT_NO_GITHUB,
//							new StudentNameModel(student.getFirstName(), student.getLastName(), true),
//							student.getClientID(), "");
				else
					System.out.println("Added new student (no Github): " + student.getFirstName() + " " + student.getLastName());
//					MySqlDbLogging.insertLogData(LogDataModel.ADD_NEW_STUDENT,
//							new StudentNameModel(student.getFirstName(), student.getLastName(), true),
//							student.getClientID(), "");
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("insertStudent: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel studentModel = new StudentNameModel(student.getFirstName(), student.getLastName(),
						student.getIsInMasterDb() == 1 ? true : false);
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, studentModel, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	private void updateStudent(StudentImportModel importStudent, StudentImportModel dbStudent) {
		for (int i = 0; i < 2; i++) {
			// Before updating database, determine what fields have changed
			String changedFields = getStudentChangedFields(importStudent, dbStudent);
			boolean newStudent = false;

			// If student added back to DB, mark as new
			if (changedFields.contains("Added back"))
				newStudent = true;

			// If student level just changed, clear module field and graduate student
			if (changedFields.contains("Current Level") || changedFields.contains("Exam Score")) {
				updateLastEventInfoByStudent(dbStudent.getClientID(), null, null, "NULL");
				graduateStudent(importStudent, dbStudent);
			}

			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection.prepareStatement(
						"UPDATE Students SET LastName=?, FirstName=?, GithubName=?, NewGithub=?, NewStudent=?,"
								+ "Gender=?, StartDate=?, Location=?, GradYear=?, isInMasterDb=?, Email=?,"
								+ "EmergencyEmail=?, AcctMgrEmail=?, Phone=?, AcctMgrPhone=?, HomePhone=?, "
								+ "EmergencyPhone=?, Birthdate=?, TASinceDate=?, TAPastEvents=?, CurrentLevel=?, "
								+ "LastScore=? WHERE ClientID=?;");

				int col = 1;
				updateStudentStmt.setString(col++, importStudent.getLastName());
				updateStudentStmt.setString(col++, importStudent.getFirstName());
				if (importStudent.getGithubName().equals(""))
					updateStudentStmt.setString(col++, null);
				else
					updateStudentStmt.setString(col++, importStudent.getGithubName());
				updateStudentStmt.setInt(col++, 0);
				updateStudentStmt.setInt(col++, newStudent ? 1 : 0);
				updateStudentStmt.setInt(col++, importStudent.getGender());
				if (importStudent.getStartDate() != null && !importStudent.getStartDate().equals(""))
					updateStudentStmt.setDate(col++, java.sql.Date.valueOf(importStudent.getStartDate()));
				else {
					updateStudentStmt.setDate(col++, null);
				}
				updateStudentStmt.setInt(col++, importStudent.getHomeLocation());
				updateStudentStmt.setInt(col++, importStudent.getGradYear());
				updateStudentStmt.setInt(col++, 1); // is in master DB
				updateStudentStmt.setString(col++, importStudent.getEmail());
				updateStudentStmt.setString(col++, importStudent.getEmergContactEmail());
				updateStudentStmt.setString(col++, importStudent.getAccountMgrEmails());
				updateStudentStmt.setString(col++, importStudent.getPhone1());
				updateStudentStmt.setString(col++, importStudent.getAccountMgrPhones());
				updateStudentStmt.setString(col++, importStudent.getPhone2());
				updateStudentStmt.setString(col++, importStudent.getEmergContactPhone());
				updateStudentStmt.setString(col++, importStudent.getBirthDate());
				updateStudentStmt.setString(col++, importStudent.getStaffSinceDate());
				updateStudentStmt.setInt(col++, importStudent.getStaffPastEvents());
				updateStudentStmt.setString(col++, importStudent.getCurrLevel());
				updateStudentStmt.setString(col++, importStudent.getLastExamScore());
				updateStudentStmt.setInt(col, importStudent.getClientID());

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();

				if (!changedFields.equals(""))
					System.out.println("Updated " + importStudent.getFirstName() + " " + importStudent.getLastName() 
							+ " " + changedFields);
//					MySqlDbLogging.insertLogData(LogDataModel.UPDATE_STUDENT_INFO,
//							new StudentNameModel(importStudent.getFirstName(), importStudent.getLastName(), true),
//							importStudent.getClientID(), changedFields);
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateStudent: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel studentModel = new StudentNameModel(importStudent.getFirstName(),
						importStudent.getLastName(), true);
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, studentModel, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	private String getStudentChangedFields(StudentImportModel importStudent, StudentImportModel dbStudent) {
		String changes = "";

		if (!importStudent.getFirstName().equals(dbStudent.getFirstName())) {
			if (changes.equals(""))
				changes += " (first name";
			else
				changes += ", first name";
		}
		if (!importStudent.getLastName().equals(dbStudent.getLastName())) {
			if (changes.equals(""))
				changes += " (last name";
			else
				changes += ", last name";
		}
		if (importStudent.getGender() != dbStudent.getGender()) {
			if (changes.equals(""))
				changes += " (gender";
			else
				changes += ", gender";
		}
		if (!importStudent.getGithubName().equals(dbStudent.getGithubName())) {
			if (changes.equals(""))
				changes += " (Github user";
			else
				changes += ", Github user";
		}
		if (importStudent.getGradYear() != dbStudent.getGradYear()) {
			if (changes.equals(""))
				changes += " (Grad year";
			else
				changes += ", Grad year";
		}
		if (importStudent.getHomeLocation() != dbStudent.getHomeLocation()) {
			if (changes.equals(""))
				changes += " (Home Location";
			else
				changes += ", Home Location";
		}
		if (!importStudent.getStartDate().equals(dbStudent.getStartDate())) {
			if (changes.equals(""))
				changes += " (Start Date";
			else
				changes += ", Start Date";
		}
		if (!importStudent.getPhone1().equals(dbStudent.getPhone1())) {
			if (changes.equals(""))
				changes += " (Mobile phone";
			else
				changes += ", Mobile phone";
		}
		if (!importStudent.getAccountMgrPhones().equals(dbStudent.getAccountMgrPhones())) {
			if (changes.equals(""))
				changes += " (Acct mgr phone";
			else
				changes += ", Acct mgr phone";
		}
		if (!importStudent.getPhone2().equals(dbStudent.getPhone2())) {
			if (changes.equals(""))
				changes += " (Home phone";
			else
				changes += ", Home phone";
		}
		if (!importStudent.getEmergContactPhone().equals(dbStudent.getEmergContactPhone())) {
			if (changes.equals(""))
				changes += " (Emerg phone";
			else
				changes += ", Emerg phone";
		}
		if (!importStudent.getEmail().equals(dbStudent.getEmail())) {
			if (changes.equals(""))
				changes += " (Student email";
			else
				changes += ", Student email";
		}
		if (!importStudent.getAccountMgrEmails().equals(dbStudent.getAccountMgrEmails())) {
			if (changes.equals(""))
				changes += " (Acct Mgr email";
			else
				changes += ", Acct Mgr email";
		}
		if (!importStudent.getEmergContactEmail().equals(dbStudent.getEmergContactEmail())) {
			if (changes.equals(""))
				changes += " (Emerg email";
			else
				changes += ", Emerg email";
		}
		if (importStudent.getIsInMasterDb() != dbStudent.getIsInMasterDb()) {
			if (changes.equals(""))
				changes += " (Added back to Master DB";
			else
				changes += ", Added back to Master DB";
		}
		if (!importStudent.getBirthDate().equals(dbStudent.getBirthDate())) {
			if (changes.equals(""))
				changes += " (Birthdate";
			else
				changes += ", Birthdate";
		}
		if (!importStudent.getStaffSinceDate().equals(dbStudent.getStaffSinceDate())) {
			if (changes.equals(""))
				changes += " (TA since date";
			else
				changes += ", TA since date";
		}
		if (!importStudent.getCurrLevel().equals(dbStudent.getCurrLevel())) {
			if (changes.equals(""))
				changes += " (Current Level " + importStudent.getCurrLevel();
			else
				changes += ", Current Level " + importStudent.getCurrLevel();
		}
		if (!importStudent.getLastExamScore().equals(dbStudent.getLastExamScore())) {
			if (changes.equals(""))
				changes += " (Last Exam Score " + importStudent.getLastExamScore();
			else
				changes += ", Last Exam Score " + importStudent.getLastExamScore();
		}

		if (!changes.equals(""))
			changes += ")";

		return changes;
	}

	public ArrayList<StudentModel> getStudentsUsingFlag(String flagName) {
		ArrayList<StudentModel> studentList = new ArrayList<StudentModel>();
		DateTime today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));

		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT * FROM Students WHERE " + flagName + " = 1;");

				ResultSet result = selectStmt.executeQuery();
				while (result.next()) {
					studentList.add(new StudentModel(result.getInt("ClientID"),
							new StudentNameModel(result.getString("FirstName"), result.getString("LastName"),
									result.getBoolean("isInMasterDb")),
							sqlDb.getAge(today, result.getString("Birthdate")), result.getString("GithubName"),
							result.getInt("Gender"), result.getDate("StartDate"), result.getInt("Location"),
							result.getInt("GradYear"), result.getString("CurrentClass"), result.getString("Email"),
							result.getString("AcctMgrEmail"), result.getString("EmergencyEmail"),
							result.getString("Phone"), result.getString("AcctMgrPhone"), result.getString("HomePhone"),
							result.getString("EmergencyPhone"), result.getString("CurrentModule"),
							result.getString("CurrentLevel"), result.getString("RegisterClass"),
							result.getDate("LastVisitDate")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getStudentsUsingFlag: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return studentList;
	}

	public void updateStudentFlags(StudentModel student, String flagName, int newFlagState) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Students SET " + flagName + "=? WHERE ClientID=?;");

				updateStudentStmt.setInt(1, newFlagState);
				updateStudentStmt.setInt(2, student.getClientID());

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateStudentFlags: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, student.getNameModel(),
						student.getClientID(), ": " + e2.getMessage());
				break;
			}
		}
	}

	private void updateIsInMasterDb(StudentImportModel student, int isInMasterDb) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Students SET isInMasterDb=? WHERE ClientID=?;");

				updateStudentStmt.setInt(1, isInMasterDb);
				updateStudentStmt.setInt(2, student.getClientID());

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateIsInMasterDb: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel model = new StudentNameModel(student.getFirstName(), student.getLastName(),
						(isInMasterDb == 1) ? true : false);
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, model, student.getClientID(),
						": " + e2.getMessage());
				break;
			}
		}
	}

	private void updateStudentLastVisit(StudentModel student, AttendanceEventModel importEvent) {
		if (importEvent.getServiceCategory().equals("class java") && !importEvent.getEventName().startsWith("EL@")) {
			String eventName = importEvent.getEventName().trim();
			char levelChar = '0';

			// Report error if no current level (assume level 0)
			if (student.getCurrentLevel().equals("")) {
				MySqlDbLogging.insertLogData(LogDataModel.MISSING_CURRENT_LEVEL, student.getNameModel(),
						student.getClientID(), ", Assuming Level 0");
			} else
				levelChar = student.getCurrentLevel().charAt(0);

			// Update student's current class and last visit time
			if (student.getLastVisitDate() == null
					|| student.getLastVisitDateString().compareTo(importEvent.getServiceDateString()) < 0) {
				updateLastEventInfoByStudent(student.getClientID(), eventName, importEvent.getServiceDateString(),
						null);
			}

			// Check if attendance event matches student's current level
			if (eventName.startsWith("Java") && (levelChar < '0' || levelChar > '8')) {
				// Class mismatch
				MySqlDbLogging.insertLogData(LogDataModel.CLASS_LEVEL_MISMATCH, student.getNameModel(),
						importEvent.getClientID(), " for " + eventName + " on " + importEvent.getServiceDateString()
								+ ", Pike13 Level = " + student.getCurrentLevel());
			}
			
		} else if (student.getLastVisitDate() != null && student.getLastVisitDateString().compareTo(importEvent.getServiceDateString()) < 0) {
			// Update last visit date for jslam, jlab, extra, and ee classes 
			updateLastEventInfoByStudent(student.getClientID(), null, importEvent.getServiceDateString(), null);
		}
	}

	private StudentModel getStudentCurrentLevel(int clientID) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT CurrentLevel, CurrentModule FROM Students WHERE ClientID=?;");
				selectStmt.setInt(1, clientID);
				ResultSet result = selectStmt.executeQuery();

				if (result.next()) {
					return new StudentModel(clientID, result.getString("CurrentLevel"),
							result.getString("CurrentModule"));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getStudentCurrentLevel: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return null;
	}

	private void updateStudentModule(int clientID, StudentModel student, String repoName) {
		// Extract module from repo name: level must match student current level
		if (repoName != null && student != null) {
			repoName = repoName.toLowerCase();
			String currLevel = student.getCurrentLevel();
			String newModuleName = null;
			
			if (!currLevel.equals("") && currLevel.charAt(0) <= '5') {
				int idx = 0;
				// Github classroom names are formatted level-0-module-3 OR level3-module1
				if (repoName.startsWith("level" + currLevel + "-module"))
					idx = 13;
				else if (repoName.startsWith("level-" + currLevel + "-module"))
					idx = 14;
				else if (repoName.startsWith("old-level" + currLevel + "-module")) {
					idx = 17;
				} else if (currLevel.equals("5") && repoName.length() > 10 && repoName.startsWith("level5-0")
						&& repoName.charAt(9) == '-') {
					idx = 8;
				} else if (currLevel.equals("2") && repoName.startsWith("processingsnake-")) {
					newModuleName="2";
				} else if (currLevel.equals("2") && repoName.startsWith("league-invaders-")) {
					newModuleName="3";
				} else if (currLevel.equals("2") && repoName.startsWith("league-level2-game-")) {
					newModuleName="4";
				} else if (repoName.startsWith("level" + currLevel + "-exam") || repoName.startsWith("level" + currLevel + "-coding-exam") || repoName.startsWith("level" + currLevel + "-codingexam")) {
					newModuleName = "E";
				} else if (repoName.startsWith("level-" + currLevel + "-coding-exam") || repoName.startsWith("level-" + currLevel + "-codingexam")) {
					newModuleName = "E";
				} else if (repoName.startsWith("level-" + currLevel + "-practice-coding-exam")) {
					newModuleName="P";
				} else if (repoName.startsWith("level" + currLevel + "-checkpoint")) {
					idx = 17;
				} else {
					System.out.println(clientID + ": (unknown) " + repoName + " [" + currLevel + "][" + student.getCurrentModule() + "]");
					return; // No matching level in repo name
				}

				if (newModuleName == null) {
					if (repoName.charAt(idx) == '-')
						idx++;

					// Now find module
					if (repoName.charAt(idx) >= '0' && repoName.charAt(idx) <= '9' // module #
						&& (repoName.length() == (idx + 1) || repoName.charAt(idx + 1) == '-')) {
						newModuleName = repoName.substring(idx, idx + 1);
					}
				}
				
				if (newModuleName != null) {
					// Done parsing repo name; update student module if changed
					if (student.getCurrentModule() == null || newModuleName.compareTo(student.getCurrentModule()) > 0 || newModuleName.equals("E")) {
						System.out.println("Update module for " + clientID + " to [" + currLevel + "][" + newModuleName + "], repo = " + repoName);
						updateLastEventInfoByStudent(clientID, null, null, newModuleName);
					}
				}
			}
		}
	}

	private void updateLastEventInfoByStudent(int clientID, String eventName, String lastVisitDate, String module) {
		boolean addModule = false;
		String separator = "";
		String updateFields = "";

		if (eventName != null) {
			updateFields = "CurrentClass=? ";
			separator = ", ";
		}
		if (module != null) {
			if (module.equals("NULL"))
				// Module cleared due to new class level
				updateFields += separator + "CurrentModule=NULL ";
			else {
				updateFields += separator + "CurrentModule=? ";
				addModule = true;
			}
		}
		if (lastVisitDate != null)
			updateFields += separator + "LastVisitDate=? ";

		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Students SET " + updateFields + "WHERE ClientID=?;");

				int col = 1;
				if (eventName != null)
					updateStudentStmt.setString(col++, eventName);
				if (addModule)
					updateStudentStmt.setString(col++, module);
				if (lastVisitDate != null)
					updateStudentStmt.setDate(col++, java.sql.Date.valueOf(lastVisitDate));
				updateStudentStmt.setInt(col, clientID);

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateLastEventInfoByStudent: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false),
						clientID, ": " + e2.getMessage());
				break;
			}
		}
	}

	/*
	 * ------- Attendance Import Database Queries -------
	 */
	public ArrayList<AttendanceEventModel> getAllEvents(String startDate) {
		ArrayList<AttendanceEventModel> eventList = new ArrayList<AttendanceEventModel>();

		for (int i = 0; i < 2; i++) {
			try {
				// Get attendance data from the DB for all students
				PreparedStatement selectStmt = sqlDb.dbConnection.prepareStatement(
						"SELECT * FROM Attendance, Students WHERE Attendance.ClientID = Students.ClientID "
								+ "AND (State = 'completed' OR State = 'registered') AND ServiceDate >= ? "
								+ "ORDER BY Attendance.ClientID ASC, ServiceDate DESC, VisitID ASC;");
				selectStmt.setString(1, startDate);
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					eventList
							.add(new AttendanceEventModel(result.getInt("ClientID"), result.getInt("VisitID"),
									result.getDate("ServiceDate"), result.getString("ServiceTime"),
									result.getString("EventName"), result.getString("GithubName"),
									result.getString("RepoName"), result.getString("Comments"), result.getString("GitDescription"),
									new StudentNameModel(result.getString("FirstName"), result.getString("LastName"),
											true),
									result.getString("ServiceCategory"), result.getString("State"),
									result.getString("LastSFState"), result.getString("TeacherNames"),
									result.getString("ClassLevel")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getAllEvents: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return eventList;
	}

	public ArrayList<AttendanceEventModel> getExpiredEvents(String sinceDate) {
		ArrayList<AttendanceEventModel> eventList = new ArrayList<AttendanceEventModel>();

		for (int i = 0; i < 2; i++) {
			try {
				// Get expired (not completed) attendance data
				PreparedStatement selectStmt = sqlDb.dbConnection.prepareStatement(
						"SELECT * FROM Attendance, Students WHERE Attendance.ClientID = Students.ClientID "
								+ "AND State = 'registered' AND ServiceDate < ?;");
				selectStmt.setString(1, sinceDate);
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					System.out.println("Removing: " + result.getInt("ClientID") + ", " + result.getDate("ServiceDate")
							 + ", " + result.getString("EventName") + ", " + result.getString("State"));

					eventList.add(new AttendanceEventModel(result.getInt("ClientID"), 
							result.getInt("VisitID"), result.getDate("ServiceDate"), 
							result.getString("ServiceTime"), result.getString("EventName"), 
							result.getString("GithubName"), result.getString("RepoName"), 
							result.getString("Comments"), result.getString("GitDescription"), 
							new StudentNameModel(result.getString("FirstName"), result.getString("LastName"), true),
							result.getString("ServiceCategory"), result.getString("State"),
							result.getString("LastSFState"), result.getString("TeacherNames"),
							result.getString("ClassLevel")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getExpiredEvents: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return eventList;
	}

	public void importAttendance(String startDate, ArrayList<AttendanceEventModel> importList, 
			                     ArrayList<StudentModel> studentList, boolean fullList) {
		// Import attendance from Pike13 to the Tracker database
		ArrayList<AttendanceEventModel> dbList = getAllEvents(startDate);
		int dbListIdx = 0;
		int dbListSize = dbList.size();
		Collections.sort(importList);
		Collections.sort(dbList);

		AttendanceEventModel dbAttendance;
		for (int i = 0; i < importList.size(); i++) {
			AttendanceEventModel importEvent = importList.get(i);
			String teachers = parseTeacherNames(importEvent.getTeacherNames());

			// If at end of DB list, then default operation is insert (1)
			int compare = 1;
			if (dbListIdx < dbListSize) {
				dbAttendance = dbList.get(dbListIdx);

				// Compare attendance; if matched, also check state & teachers
				compare = dbAttendance.compareTo(importEvent);
				boolean eventChanged = (!importEvent.getEventName().contains("(") && !dbAttendance.getEventName().trim().equals(importEvent.getEventName().trim()));
				if (compare == 0 && (!dbAttendance.getState().equals(importEvent.getState())
						|| !dbAttendance.getTeacherNames().equals(teachers)
						|| eventChanged
						|| (dbAttendance.getServiceTime().equals("") && !importEvent.getServiceTime().equals("")))) {
					// Debug: track changing event names
					if (eventChanged)
						System.out.println("Event name changed: " + importEvent.getServiceDateString() + ", " + dbAttendance.getEventName().trim() 
								+ " (" + dbAttendance.getVisitID() + ") to " + importEvent.getEventName().trim() + " (" + importEvent.getVisitID()
								+ ") [" + importEvent.getStudentNameModel().getFirstName() + ", " + importEvent.getClientID() + ", " + importEvent.getState() + "]");
					compare = 2;
				}
			} else
				dbAttendance = null;

			if (compare == 0) {
				// All data matches, so continue through list
				dbListIdx++;
				continue;

			} else if (compare == -1) {
				// Extra events in DB; toss data until caught up with import list
				while (dbListIdx < dbListSize && dbList.get(dbListIdx).compareTo(importEvent) < 0) {
					// Delete registered events that were canceled
					if (fullList && dbList.get(dbListIdx).getState().equals("registered")
							&& dbList.get(dbListIdx).getServiceCategory().startsWith("class"))
						dbList.get(dbListIdx).setMarkForDeletion(true);

					dbListIdx++;
				}

				// Caught up, now compare again and process
				compare = 1;
				if (dbListIdx < dbListSize) {
					dbAttendance = dbList.get(dbListIdx);

					// Compare attendance; if matched, also check state & teachers
					compare = dbAttendance.compareTo(importEvent);
					boolean eventChanged = (!importEvent.getEventName().contains("(") && !dbAttendance.getEventName().trim().equals(importEvent.getEventName().trim()));
					if (compare == 0 && (!dbAttendance.getState().equals(importEvent.getState())
							|| !dbAttendance.getTeacherNames().equals(teachers)
							|| eventChanged
							|| (dbAttendance.getServiceTime().equals("") && !importEvent.getServiceTime().equals("")))) {
						// Debug: track changing event names
						if (eventChanged)
							System.out.println("Event name changed: " + importEvent.getServiceDateString() + ", " + dbAttendance.getEventName().trim() 
									+ " (" + dbAttendance.getVisitID() + ") to " + importEvent.getEventName().trim() + " (" + importEvent.getVisitID()
									+ ") [" + importEvent.getStudentNameModel().getFirstName() + ", " + importEvent.getClientID() + ", " + importEvent.getState() + "]");
						compare = 2;
					}
				} else
					dbAttendance = null;

				if (compare == 0) {
					dbListIdx++;

				} else {
					int idx = getClientIdxInStudentList(studentList, importEvent.getClientID());
					if (idx >= 0) {
						if (compare == 1)
							addAttendance(importEvent, teachers, studentList.get(idx));
						else // state field has changed, so update
							updateAttendanceState(importEvent, dbAttendance, teachers, studentList.get(idx));

					} else
						MySqlDbLogging.insertLogData(LogDataModel.STUDENT_NOT_FOUND,
								new StudentNameModel(importEvent.getStudentNameModel().getFirstName(), "", false),
								importEvent.getClientID(),
								": " + importEvent.getEventName().trim() + " on " + importEvent.getServiceDateString());
				}

			} else {
				// Data does not match existing student
				int idx = getClientIdxInStudentList(studentList, importEvent.getClientID());

				if (idx >= 0) {
					// Student exists in DB, so add attendance data for this student
					if (compare == 1)
						addAttendance(importEvent, teachers, studentList.get(idx));
					else // state field has changed, so update
						updateAttendanceState(importEvent, dbAttendance, teachers, studentList.get(idx));

				} else {
					// Student not found
					MySqlDbLogging.insertLogData(LogDataModel.STUDENT_NOT_FOUND,
							new StudentNameModel(importEvent.getStudentNameModel().getFirstName(), "", false),
							importEvent.getClientID(),
							": " + importEvent.getEventName().trim() + " on " + importEvent.getServiceDateString());
				}
			}
		}

		if (fullList) {
			// Delete registered classes that were canceled
			String today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles")).toString("yyyy-MM-dd");
			for (AttendanceEventModel m : dbList) {
				if (m.isMarkForDeletion() && m.getServiceDateString().compareTo(today) >= 0) {
					deleteFromAttendance(m.getClientID(), m.getVisitID(), m.getStudentNameModel());
				}
			}
		}
	}

	public void updateAttendance(int clientID, StudentNameModel nameModel, String serviceDate, String eventName,
			String repoName, String comments, String gitDescription) {
		PreparedStatement updateAttendanceStmt;
		for (int i = 0; i < 2; i++) {
			try {
				// The only fields that should be updated are the comments and repo name
				updateAttendanceStmt = sqlDb.dbConnection.prepareStatement(
						"UPDATE Attendance SET Comments=?, RepoName=?, GitDescription=? WHERE ClientID=? AND ServiceDate=?;");

				int col = 1;
				if (comments != null && comments.length() >= COMMENT_WIDTH)
					comments = comments.substring(0, COMMENT_WIDTH);
				updateAttendanceStmt.setString(col++, comments);
				if (repoName != null && repoName.length() >= REPO_NAME_WIDTH)
					repoName = repoName.substring(0, REPO_NAME_WIDTH);
				updateAttendanceStmt.setString(col++, repoName);
				if (gitDescription != null && gitDescription.length() >= COMMENT_WIDTH)
					updateAttendanceStmt.setString(col++, gitDescription.substring(0, COMMENT_WIDTH));
				else
					updateAttendanceStmt.setString(col++, gitDescription);
				updateAttendanceStmt.setInt(col++, clientID);
				updateAttendanceStmt.setDate(col++, java.sql.Date.valueOf(serviceDate));

				updateAttendanceStmt.executeUpdate();
				updateAttendanceStmt.close();

				// Now update student latest module using repo name
				updateStudentModule(clientID, getStudentCurrentLevel(clientID), repoName);
				return;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateAttendance: " + clientID + " " + serviceDate + ", " + eventName + ", " +  e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				StudentNameModel studentModel = new StudentNameModel(nameModel.getFirstName(), nameModel.getLastName(),
						nameModel.getIsInMasterDb());
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, studentModel, clientID,
						": " + e.getMessage());
			}
		}
	}

	public void deleteExpiredAttendance(ArrayList<AttendanceEventModel> attendList) {
		for (AttendanceEventModel a : attendList) {
			deleteFromAttendance(a.getClientID(), a.getVisitID(), a.getStudentNameModel());
		}
	}

	public void createSortedAttendanceList() {
		for (int i = 0; i < 2; i++) {
			try {
				// Empty the sorted list
				PreparedStatement truncateStmt = sqlDb.dbConnection
						.prepareStatement("TRUNCATE TABLE SortedAttendance;");
				truncateStmt.executeUpdate();
				truncateStmt.close();

				// Now re-sort in descending date order
				PreparedStatement insertStmt = sqlDb.dbConnection.prepareStatement("INSERT INTO SortedAttendance "
						+ "SELECT * FROM Attendance ORDER BY ClientID, ServiceDate DESC, EventName;");
				insertStmt.executeUpdate();
				insertStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("createSortedAttendanceList: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						" sorting: " + e2.getMessage());
				break;
			}
		}
	}

	public ArrayList<AttendanceEventModel> getEventsWithNoComments(String startDate, int clientID,
			boolean includeEmpty) {
		ArrayList<AttendanceEventModel> eventList = new ArrayList<AttendanceEventModel>();

		// Can either filter on null comments or both null + empty comments.
		// Null comments transition to empty comments to mark them as processed.
		String clientIdFilter = "";
		if (clientID != 0) // Specific github user being updated
			clientIdFilter = "Students.ClientID = " + clientID + " AND ";

		if (includeEmpty)
			clientIdFilter += "(Comments IS NULL OR Comments = '') ";
		else
			clientIdFilter += "Comments IS NULL AND LEFT(EventName,2) != 'EL' AND LEFT(EventName,2) != 'PG' ";

		for (int i = 0; i < 2; i++) {
			try {
				// Get attendance data from the DB for all students that have a github user name
				// and the comment field is blank
				PreparedStatement selectStmt = sqlDb.dbConnection.prepareStatement(
						"SELECT * FROM Attendance, Students WHERE Attendance.ClientID = Students.ClientID AND "
								+ clientIdFilter + " AND State = 'completed' AND (CurrentLevel = '' OR CurrentLevel <= '5') "
								+ "AND GithubName IS NOT NULL AND ServiceDate >= ? ORDER BY GithubName;");
				selectStmt.setDate(1, java.sql.Date.valueOf(startDate));
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					eventList
							.add(new AttendanceEventModel(result.getInt("ClientID"), result.getInt("VisitID"),
									result.getDate("ServiceDate"), result.getString("ServiceTime"),
									result.getString("EventName"), result.getString("GithubName"),
									result.getString("RepoName"), result.getString("Comments"), result.getString("GitDescription"),
									new StudentNameModel(result.getString("FirstName"), result.getString("LastName"),
											true),
									result.getString("ServiceCategory"), result.getString("State"),
									result.getString("LastSFState"), result.getString("TeacherNames"),
									result.getString("ClassLevel")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getEventsWithNoComments: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return eventList;
	}

	public ArrayList<PendingGithubModel> getPendingGithubEvents() {
		ArrayList<PendingGithubModel> eventList = new ArrayList<PendingGithubModel>();

		for (int i = 0; i < 2; i++) {
			try {
				// Get pending github event data from the DB. This data is populated by
				// a Github "hook" each time a student commits to a github classroom.
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT * FROM PendingGithub ORDER BY GitUser;");
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					// Detect dates that are GMT and convert them to Pacific time
					String date = result.getString("ServiceDate");
					if (date.length() == 20 && date.charAt(19) == 'Z')
						date = (new DateTime(date).withZone(DateTimeZone.forID("America/Los_Angeles"))).toString("yyyy-MM-dd HH:mm:ss");

					eventList.add(new PendingGithubModel(result.getInt("PrimaryID"), result.getString("GitUser"),
							result.getString("RepoName"), date, result.getString("Comments")));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getPendingGithubEvents: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.GITHUB_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e.getMessage());
				break;
			}
		}
		return eventList;
	}

	private void deletePendingGithubEvent(int primaryID) {
		PreparedStatement deleteGithubStmt;
		for (int i = 0; i < 2; i++) {
			try {
				deleteGithubStmt = sqlDb.dbConnection.prepareStatement("DELETE FROM PendingGithub WHERE PrimaryID=?;");
				deleteGithubStmt.setInt(1, primaryID);
				deleteGithubStmt.executeUpdate();
				deleteGithubStmt.close();

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("deletePendingGithubEvent: " + e1.getMessage());
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.GITHUB_DB_ERROR, new StudentNameModel("", "", false), 0,
						" deleting record: " + e.getMessage());
				break;
			}
		}
	}

	public void updatePendingGithubComments(ArrayList<PendingGithubModel> githubList, String startDate,
			ArrayList<AttendanceEventModel> attendList, ArrayList<AttendanceEventModel> incompAttendList, ArrayList<StudentModel> studentList) {
		
		int origSize = githubList.size();
		clearAllPendGithubStatus();
		
		// Process all the pending commits
		for (int i = 0; i < githubList.size(); i++) {
			// Get commit date
			PendingGithubModel pendingGit = githubList.get(i);
			String gitUser = pendingGit.getGitUser().toLowerCase().trim();
			String commitDate = pendingGit.getServiceDate().substring(0, 10);

			// Record is out-of-date (attendance never updated!), or teacher, so remove
			if (commitDate.compareTo(startDate) < 0 || gitUser.equals("wavis421") || gitUser.equals("keithagroves")
					|| gitUser.equals("davedleague") || gitUser.equals("sebastiantroncoso93")
					|| gitUser.equals("codybsauer") || gitUser.equals("shacheeparikh") || gitUser.equals("tylercwiltz")
					|| gitUser.equals("colbyschexnayder")
					|| gitUser.equals("mwoguri")      // Michiyo
					|| gitUser.equals("jaleague")     // Jackie
					|| gitUser.equals("zaelix")       // Mike
					|| gitUser.equals("dencee")       // Daniel
					|| gitUser.equals("mjfre")        // Matt
					|| gitUser.equals("awasicek")     // Andrew
					|| gitUser.equals("dkunkler")     // David Kunkler, volunteer (parent of 3 students)
					|| gitUser.equals("rey-rey"))     // Rey Fidel, volunteer teacher
			{
				deletePendingGithubEvent(pendingGit.getPrimaryID());
				githubList.remove(i);
				i--;
				continue;
			}

			// Find gituser & date match in event list; append multiple comments
			boolean foundMatch = false;
			for (int j = 0; j < attendList.size(); j++) {
				AttendanceEventModel event = attendList.get(j);
				if (commitDate.equals(event.getServiceDateString())
						&& gitUser.equals(event.getGithubName().toLowerCase())) {
					// Update comments & repo name
					event.setGithubComments(pendingGit.getComments());
					updateAttendance(event.getClientID(), event.getStudentNameModel(), commitDate, event.getEventName().trim(),
							pendingGit.getRepoName(), event.getGithubComments(), event.getGitDescription());
					foundMatch = true;
				}
			}
			if (foundMatch) {
				// This pending github record has been processed, so remove!
				deletePendingGithubEvent(pendingGit.getPrimaryID());
				githubList.remove(i);
				i--;
			}
			else {
				// No completed attendance found for this GitUser; now check incomplete attendance
				StudentModel stud = findGithubUser (studentList, gitUser);
				if (stud == null) {
					// This GitUser is not attached to any student in the DB, so mark as missing
					setMissingGithub (pendingGit);
				}
				else {
					// Found student, so search incomplete attendance and update state in pending Github table
					for (AttendanceEventModel att : incompAttendList) {
						if (commitDate.equals(att.getServiceDateString()) && att.getClientID() == stud.getClientID())
							setGithubStatus (pendingGit, att.getState());
					}
				}
			}
		}
		
		if (origSize > githubList.size())
			System.out.println((origSize - githubList.size()) + " pending github processed");
	}
	
	private int addAttendance(AttendanceEventModel importEvent, String teacherNames, StudentModel student) {
		// Update class level if <= L8
		boolean addLevel = false;
		String today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles")).toString("yyyy-MM-dd");

		if (importEvent.getState().equals("completed") && importEvent.getServiceDateString().compareTo(today) <= 0
				&& (importEvent.getEventName().startsWith("AD") || importEvent.getEventName().startsWith("AG")
						|| importEvent.getEventName().startsWith("PG") || importEvent.getEventName().startsWith("Java@")
						|| (importEvent.getServiceCategory().startsWith("class ")
								&& !student.getCurrentLevel().equals("") && student.getCurrentLevel().charAt(0) <= '8'))) {
			// Update student's current level and last visit date
			addLevel = true;
			updateStudentLastVisit(student, importEvent);
		}

		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement addAttendanceStmt;
				if (addLevel)
					addAttendanceStmt = sqlDb.dbConnection.prepareStatement(
							"INSERT INTO Attendance (ClientID, ServiceDate, ServiceTime, EventName, VisitID, TeacherNames, "
									+ "ServiceCategory, State, ClassLevel) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
				else
					addAttendanceStmt = sqlDb.dbConnection.prepareStatement(
							"INSERT INTO Attendance (ClientID, ServiceDate, ServiceTime, EventName, VisitID, TeacherNames, "
									+ "ServiceCategory, State) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

				int col = 1;
				addAttendanceStmt.setInt(col++, importEvent.getClientID());
				addAttendanceStmt.setDate(col++, java.sql.Date.valueOf(importEvent.getServiceDateString()));
				addAttendanceStmt.setString(col++, importEvent.getServiceTime());
				addAttendanceStmt.setString(col++, importEvent.getEventName().trim());
				addAttendanceStmt.setInt(col++, importEvent.getVisitID());
				addAttendanceStmt.setString(col++, teacherNames);
				addAttendanceStmt.setString(col++, importEvent.getServiceCategory());
				addAttendanceStmt.setString(col++, importEvent.getState());
				if (addLevel)
					addAttendanceStmt.setString(col, student.getCurrentLevel());

				addAttendanceStmt.executeUpdate();
				addAttendanceStmt.close();

				// Now update student's last visit date (if not already done)
				if (!addLevel)
					updateLastVisitDate(importEvent, student, today);
				return 1;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("addAttendance: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLIntegrityConstraintViolationException e2) {
				// Attendance data already exists, do nothing
				break;

			} catch (SQLException | NullPointerException e3) {
				e3.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, student.getNameModel(),
						importEvent.getClientID(), ": " + e3.getMessage());
				break;
			}
		}
		return 0;
	}

	private int updateAttendanceState(AttendanceEventModel importEvent, AttendanceEventModel dbAttendance,
			String teachers, StudentModel student) {
		// When transitioning to completed, update current level for students <= L8.
		// Only set level for Slams & Make-ups.
		boolean addLevel = false;
		String today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles")).toString("yyyy-MM-dd");

		if ((dbAttendance == null || !dbAttendance.getState().equals("completed"))
				&& importEvent.getState().equals("completed") && importEvent.getServiceDateString().compareTo(today) <= 0
				&& (importEvent.getEventName().startsWith("Java@")
						|| (importEvent.getServiceCategory().startsWith("class ")
								&& !student.getCurrentLevel().equals("") && student.getCurrentLevel().charAt(0) <= '8'))) {
			addLevel = true;
			updateStudentLastVisit(student, importEvent);
		}

		PreparedStatement updateAttendanceStmt;
		for (int i = 0; i < 2; i++) {
			try {
				// The only fields that should be updated are the State & Teacher fields
				if (addLevel)
					updateAttendanceStmt = sqlDb.dbConnection.prepareStatement(
							"UPDATE Attendance SET State=?, TeacherNames=?, ServiceTime=?, ClassLevel=?, EventName=? "
									+ "WHERE ClientID=? AND VisitID=? AND ServiceDate=? AND (ServiceTime='' OR ServiceTime=?);");
				else
					updateAttendanceStmt = sqlDb.dbConnection
							.prepareStatement("UPDATE Attendance SET State=?, TeacherNames=?, ServiceTime=?, EventName=? "
									+ "WHERE ClientID=? AND VisitID=? AND ServiceDate=? AND (ServiceTime='' OR ServiceTime=?);");
				int col = 1;
				updateAttendanceStmt.setString(col++, importEvent.getState());
				updateAttendanceStmt.setString(col++, teachers);
				updateAttendanceStmt.setString(col++, importEvent.getServiceTime());
				if (addLevel)
					updateAttendanceStmt.setString(col++, student.getCurrentLevel());
				updateAttendanceStmt.setString(col++,  importEvent.getEventName().trim());
				updateAttendanceStmt.setInt(col++, importEvent.getClientID());
				updateAttendanceStmt.setInt(col++, importEvent.getVisitID());
				updateAttendanceStmt.setDate(col++, java.sql.Date.valueOf(importEvent.getServiceDateString()));
				updateAttendanceStmt.setString(col++, importEvent.getServiceTime());

				updateAttendanceStmt.executeUpdate();
				updateAttendanceStmt.close();

				// Now update student's last visit date (if not already done)
				if (!addLevel)
					updateLastVisitDate(importEvent, student, today);
				return 1;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateAttendanceState: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, student.getNameModel(),
						importEvent.getClientID(), ": " + e.getMessage());
				break;
			}
		}
		return 0;
	}

	private void updateLastVisitDate(AttendanceEventModel importEvent, StudentModel student, String today) {
		if (importEvent.getState().equals("completed")
				&& importEvent.getServiceDateString().compareTo(today) <= 0
				&& (student.getLastVisitDate() == null ||
				    student.getLastVisitDateString().compareTo(importEvent.getServiceDateString()) < 0)) {
			// Update last visit date for this student
			if (importEvent.getEventName().toLowerCase().contains("leave")) {
				// For student on LOA, do not update last visit date but set current class to LOA
				System.out.println("LOA: " + student.getClientID() + ", " + student.getFirstName() + " " + student.getLastName() 
					+ ", " + importEvent.getServiceDateString() + ", " + importEvent.getEventName());
				updateLastEventInfoByStudent(student.getClientID(), "LOA", null, null);
			}
			else
				updateLastEventInfoByStudent(student.getClientID(), null, importEvent.getServiceDateString(), null);
		}
	}

	private void deleteFromAttendance(int clientID, int visitID, StudentNameModel studentModel) {
		PreparedStatement deleteAttendanceStmt;
		for (int i = 0; i < 2; i++) {
			try {
				deleteAttendanceStmt = sqlDb.dbConnection
						.prepareStatement("DELETE FROM Attendance WHERE ClientID=? AND VisitID=?;");

				deleteAttendanceStmt.setInt(1, clientID);
				deleteAttendanceStmt.setInt(2, visitID);

				deleteAttendanceStmt.executeUpdate();
				deleteAttendanceStmt.close();

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("deleteFromAttendance: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, studentModel, clientID,
						" removing registered attendance record: " + e.getMessage());
				break;
			}
		}
	}

	public void updateAttendLevelChanges(int visitID, String state) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateAttendanceStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Attendance SET LastSFState = ? WHERE VisitID = ?;");

				updateAttendanceStmt.setString(1, state);
				updateAttendanceStmt.setInt(2, visitID);

				updateAttendanceStmt.executeUpdate();
				updateAttendanceStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateAttendLevelChanges: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						" updating LastSFState: " + e2.getMessage());
				break;
			}
		}
	}

	/*
	 * ------- Graduation Import Database Queries -------
	 */
	public void updateGradInSfField(int clientID, String studentName, String clientLevelKey, boolean newValue) {
		// Graduation records are uniquely identified by clientID & level pair.
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateGraduateStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Graduation SET InSalesForce=? WHERE ClientID=? AND GradLevel=?;");

				updateGraduateStmt.setInt(1, newValue ? 1 : 0);
				updateGraduateStmt.setInt(2, clientID);
				updateGraduateStmt.setInt(3, Integer.parseInt(clientLevelKey.substring(7)));  // Level is appended after 7-digit ClientID

				updateGraduateStmt.executeUpdate();
				updateGraduateStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateGradInSfField: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR,
						new StudentNameModel("", "", false), clientID, " for Graduation: " + e2.getMessage());
				break;
			}
		}
	}

	private void graduateStudent(StudentImportModel importStudent, StudentImportModel dbStudent) {
		// Add record to Graduation table if current level has increased
		if (!dbStudent.getCurrLevel().equals("")
				&& (importStudent.getCurrLevel().compareTo(dbStudent.getCurrLevel()) > 0
						|| (importStudent.getCurrLevel().equals(dbStudent.getCurrLevel())
								&& importStudent.getLastExamScore().length() > 3
								&& !dbStudent.getLastExamScore().equals(importStudent.getLastExamScore())))) {
			// Expected score format is (example): "L1 85.3"
			String score = "";
			boolean isPromoted = false, isSkip = false;
			int dbCurrLevelNum = Integer.parseInt(dbStudent.getCurrLevel());
			int importLevelNum = Integer.parseInt(importStudent.getCurrLevel());

			if (importStudent.getLastExamScore().toLowerCase().contains("promoted"))
				isPromoted = true; // Student did not pass the exam
			else if (importStudent.getLastExamScore().toLowerCase().contains("skip"))
				isSkip = true;

			if (isSkip && importLevelNum > dbCurrLevelNum 
					&& importStudent.getLastExamScore().startsWith("L" + ((Integer) (importLevelNum - 1)).toString() + " ")) {
				// Student has skipped one or more levels; graduate each level
				graduateSkippedLevels(dbStudent, importLevelNum);
				return;
			}

			else if (importLevelNum == (dbCurrLevelNum + 1) && dbCurrLevelNum <= 8
					&& importStudent.getLastExamScore().startsWith("L" + dbStudent.getCurrLevel() + " ")) {
				// New graduate: If score just 'Ln ' or student promoted/skipped, then no score
				if (!isPromoted && !isSkip && importStudent.getLastExamScore().length() > 3)
					score = importStudent.getLastExamScore().substring(3);

			} else if (importLevelNum == dbCurrLevelNum && dbCurrLevelNum > 0
					&& importStudent.getLastExamScore().startsWith("L" + (dbCurrLevelNum - 1) + " ")) {
				// Student already graduated, score being updated
				if (!isPromoted && !isSkip)
					score = importStudent.getLastExamScore().substring(3);
				dbCurrLevelNum -= 1;

			} else if (importStudent.getLastExamScore().startsWith("AP CompA")) {
				// No level switch for AP
				importStudent.setCurrLevel(dbStudent.getCurrLevel());
				dbCurrLevelNum = GraduationModel.AP_COMPA_EXAM;
				if (importStudent.getLastExamScore().length() > 8)
					score = importStudent.getLastExamScore().substring(8).trim();
				System.out.println("AP CompA Exam: " + dbStudent.getFirstName() + " " + dbStudent.getLastName() + ", Curr Level " 
					+ dbStudent.getCurrLevel() + ", Score " + score);
				
			} else if (importStudent.getLastExamScore().startsWith("AP Princ")) {
				// No level switch for AP
				importStudent.setCurrLevel(dbStudent.getCurrLevel());
				dbCurrLevelNum = GraduationModel.AP_PRINC_EXAM;
				if (importStudent.getLastExamScore().length() > 8)
					score = importStudent.getLastExamScore().substring(8).trim();
				System.out.println("AP Principles Exam: " + dbStudent.getFirstName() + " " + dbStudent.getLastName() + ", Curr Level " 
					+ dbStudent.getCurrLevel() + ", Score " + score);
				
			} else if (importStudent.getLastExamScore().startsWith("Oracle")) {
				// No level switch for Oracle
				importStudent.setCurrLevel(dbStudent.getCurrLevel());
				dbCurrLevelNum = GraduationModel.ORACLE_EXAM;
				if (importStudent.getLastExamScore().length() > 6)
					score = importStudent.getLastExamScore().substring(6).trim();
				System.out.println("Oracle Exam: " + dbStudent.getFirstName() + " " + dbStudent.getLastName() + ", Curr Level " 
					+ dbStudent.getCurrLevel() + ", Score " + score);
					
			} else {
				MySqlDbLogging.insertLogData(LogDataModel.EXAM_SCORE_INVALID,
						new StudentNameModel(dbStudent.getFirstName(), dbStudent.getLastName(), true),
						dbStudent.getClientID(),
						" for Level " + dbStudent.getCurrLevel() + ": " + importStudent.getLastExamScore());
				importStudent.setCurrLevel(dbStudent.getCurrLevel());
				return;
			}

			// Add graduation record to database
			addGraduationRecord(new GraduationModel(dbStudent.getClientID(), dbStudent.getFullName(), dbCurrLevelNum,
					score, dbStudent.getCurrClass(),
					getStartDateByClientIdAndLevel(dbStudent.getClientID(), dbCurrLevelNum),
					new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles")).toString("yyyy-MM-dd"), false,
					isSkip, isPromoted));

		} else if (dbStudent.getCurrLevel().equals("")
				&& importStudent.getLastExamScore().toLowerCase().contains("skip")
				&& importStudent.getLastExamScore().charAt(0) == 'L'
				&& importStudent.getLastExamScore().charAt(2) == ' ') {
			// Student's current level is blank but student skipped level(s)
			graduateSkippedLevels(dbStudent, Integer.parseInt(importStudent.getCurrLevel()));
		}
	}

	private void graduateSkippedLevels(StudentImportModel dbStudent, int newLevel) {
		String today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles")).toString("yyyy-MM-dd");

		// Graduate each level skipped, up to 'newLevel'
		for (int i = 0; i < newLevel; i++) {
			addGraduationRecord(new GraduationModel(dbStudent.getClientID(), dbStudent.getFullName(), i, "",
					dbStudent.getCurrClass(), today, today, false, true, false));
		}
	}

	public void addGraduationRecord(GraduationModel gradModel) {
		for (int i = 0; i < 2; i++) {
			try {
				// Insert graduation record into database
				String cmdString = "INSERT INTO Graduation (ClientID, GradLevel, SkipLevel, Promoted, EndDate, CurrentClass";
				String values = ") VALUES (?, ?, ?, ?, ?, ?";

				// Don't update dates or score if no data
				if (!gradModel.getStartDate().equals("")) {
					cmdString += ", StartDate";
					values += ", ?";
				}
				if (!gradModel.getScore().equals("")) {
					cmdString += ", Score";
					values += ", ?";
				}

				// Now add graduation info to database
				PreparedStatement addGrad = sqlDb.dbConnection.prepareStatement(cmdString + values + ");");

				// Fill in the input fields
				int col = 1;
				addGrad.setInt(col++, gradModel.getClientID());
				addGrad.setInt(col++, gradModel.getGradLevel());
				addGrad.setBoolean(col++, gradModel.isSkipLevel());
				addGrad.setBoolean(col++, gradModel.isPromoted());
				addGrad.setDate(col++, java.sql.Date.valueOf(gradModel.getEndDate()));
				addGrad.setString(col++, gradModel.getCurrentClass());
				if (!gradModel.getStartDate().equals(""))
					addGrad.setDate(col++, java.sql.Date.valueOf(gradModel.getStartDate()));
				if (!gradModel.getScore().equals(""))
					addGrad.setString(col++, gradModel.getScore());

				// Execute update
				addGrad.executeUpdate();
				addGrad.close();
				break;

			} catch (MySQLIntegrityConstraintViolationException e0) {
				// Record already exists in database, so update instead
				updateGraduationRecord(gradModel);
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("addGraduationRecord: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false),
						gradModel.getClientID(), " for Graduation: " + e2.getMessage());
				break;
			}
		}
	}

	private void updateGraduationRecord(GraduationModel gradModel) {
		// Graduation records are uniquely identified by clientID & level pair.
		// Update EndDate/Score/SkipLevel/Promoted. Set 'in SF' false to force update.
		for (int i = 0; i < 2; i++) {
			try {
				// Update graduation record in database
				String cmdString = "UPDATE Graduation SET EndDate=?, " + MySqlDatabase.GRAD_MODEL_IN_SF_FIELD + "=0";

				// Only update fields if valid
				if (!gradModel.getScore().equals(""))
					cmdString += ", Score=?";
				if (gradModel.isSkipLevel())
					cmdString += ", SkipLevel=?";
				if (gradModel.isPromoted())
					cmdString += ", Promoted=?";

				// Update database
				PreparedStatement updateGraduateStmt = sqlDb.dbConnection
						.prepareStatement(cmdString + " WHERE ClientID=? AND GradLevel=?;");

				// Fill in the input fields
				int col = 1;
				updateGraduateStmt.setDate(col++, java.sql.Date.valueOf(gradModel.getEndDate()));
				if (!gradModel.getScore().equals(""))
					updateGraduateStmt.setString(col++, gradModel.getScore());
				if (gradModel.isSkipLevel())
					updateGraduateStmt.setBoolean(col++, true);
				if (gradModel.isPromoted())
					updateGraduateStmt.setBoolean(col++, true);
				updateGraduateStmt.setInt(col++, gradModel.getClientID());
				updateGraduateStmt.setInt(col++, gradModel.getGradLevel());

				// Execute update
				updateGraduateStmt.executeUpdate();
				updateGraduateStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateGraduationRecord: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false),
						gradModel.getClientID(), " for Graduation: " + e2.getMessage());
				break;
			}
		}
	}

	public void removeProcessedGraduations() {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection.prepareStatement("SELECT * FROM Graduation;");
				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					// When "in salesforce" flag is true, record can be removed
					if (result.getBoolean(MySqlDatabase.GRAD_MODEL_IN_SF_FIELD)) {
						// Graduation record has been processed, so remove from DB
						PreparedStatement deleteGradStmt = sqlDb.dbConnection
								.prepareStatement("DELETE FROM Graduation WHERE ClientID=? AND GradLevel=?;");

						// Delete student
						deleteGradStmt.setInt(1, result.getInt("ClientID"));
						deleteGradStmt.setInt(2, result.getInt("GradLevel"));
						deleteGradStmt.executeUpdate();
						deleteGradStmt.close();
					}
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("removeProcessedGraduations: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						" for Graduation: " + e2.getMessage());
				break;
			}
		}
	}

	private String getStartDateByClientIdAndLevel(int clientID, int level) {
		String levelString = ((Integer) level).toString();

		// Start date doesn't apply to AP or Oracle exams
		if (level > 8)
			return "";
		
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT ServiceDate, EventName, ClassLevel FROM Attendance "
								+ "WHERE ClientID = ? AND State = 'completed' ORDER BY ServiceDate ASC;");
				selectStmt.setInt(1, clientID);

				ResultSet result = selectStmt.executeQuery();

				while (result.next()) {
					String eventName = result.getString("EventName");
					String classLevel = result.getString("ClassLevel");
					if ((eventName.charAt(1) == '@' && eventName.startsWith(levelString))
							|| classLevel.equals(levelString)) {
						String startDate = result.getDate("ServiceDate").toString();
						if (startDate.compareTo("2017-09-30") < 0)
							return "";
						else
							return startDate;
					}
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("getStartDateByClientIdAndLevel: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.ATTENDANCE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
		return "";
	}

	/*
	 * ------- Class Schedule Import Database Queries -------
	 */
	public void importSchedule(ArrayList<ScheduleModel> importList) {
		ArrayList<ScheduleModel> dbList = sqlDb.getClassSchedule();
		int dbListIdx = 0;
		int dbListSize = dbList.size();

		Collections.sort(importList);

		for (int i = 0; i < importList.size(); i++) {
			ScheduleModel importEvent = importList.get(i);

			// If at end of DB list, then default operation is insert (1)
			int compare = 1;
			if (dbListIdx < dbListSize)
				compare = dbList.get(dbListIdx).compareTo(importEvent);

			if (compare == 0) {
				// Class data matches, check misc fields
				if (!dbList.get(dbListIdx).miscSchedFieldsMatch(importEvent))
					updateClassInSchedule(dbList.get(dbListIdx), importEvent);
				dbListIdx++;

			} else if (compare > 0) {
				// Insert new event into DB
				addClassToSchedule(importEvent);

			} else {
				// Extra event(s) in database, so delete them
				while (compare < 0) {
					removeClassFromSchedule(dbList.get(dbListIdx));
					dbListIdx++;

					if (dbListIdx < dbListSize)
						// Continue to compare until dbList catches up
						compare = dbList.get(dbListIdx).compareTo(importEvent);
					else
						// End of database list, insert remaining imports
						compare = 1;
				}
				// One final check to get in sync with importEvent
				if (compare == 0) {
					// Match, so check misc fields then continue through list
					if (!dbList.get(dbListIdx).miscSchedFieldsMatch(importEvent))
						updateClassInSchedule(dbList.get(dbListIdx), importEvent);
					dbListIdx++;

				} else {
					// Insert new event into DB
					addClassToSchedule(importEvent);
				}
			}
		}

		// Delete extra entries at end of dbList
		while (dbListIdx < dbListSize) {
			removeClassFromSchedule(dbList.get(dbListIdx));
			dbListIdx++;
		}
	}

	public void updateMissingCurrentClass() {
		DateTime today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));
		DateTime endDate = today.plusDays(7);

		for (int i = 0; i < 2; i++) {
			try {
				// Get next registered class for student
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT Students.ClientID, EventName "
								+ "FROM Attendance, Students WHERE Attendance.ClientID = Students.ClientID "
								+ "AND CurrentClass = '' AND State = 'registered' AND ServiceDate >= ? AND ServiceDate <= ? "
								+ "ORDER BY Students.ClientID, ServiceDate ASC;");
				selectStmt.setString(1, today.toString("yyyy-MM-dd"));
				selectStmt.setString(2, endDate.toString("yyyy-MM-dd"));
				ResultSet result = selectStmt.executeQuery();

				String lastClientID = "";
				while (result.next()) {
					String thisClientID = result.getString("Students.ClientID");
					if (thisClientID.equals(lastClientID))
						continue;

					lastClientID = thisClientID;
					updateLastEventInfoByStudent(Integer.parseInt(thisClientID), result.getString("EventName"), null,
							null);
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateMissingCurrentClass: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel model = new StudentNameModel("", "", false);
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, model, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	public void updateRegisteredClass() {
		DateTime today = new DateTime().withZone(DateTimeZone.forID("America/Los_Angeles"));
		DateTime endDate = today.plusDays(7);

		// First clear all currently registered classes
		clearRegisteredClasses();

		for (int i = 0; i < 2; i++) {
			try {
				// Get next registered class for student.
				// A registered class is only updated when different from the current class.
				PreparedStatement selectStmt = sqlDb.dbConnection
						.prepareStatement("SELECT Students.ClientID, EventName "
								+ "FROM Attendance, Students WHERE Attendance.ClientID = Students.ClientID "
								+ "AND LEFT(EventName,4) = 'Java' "
								+ "AND CurrentClass != EventName AND State = 'registered' "
								+ "AND ServiceDate >= ? AND ServiceDate <= ? ORDER BY Students.ClientID, ServiceDate ASC;");
				selectStmt.setString(1, today.toString("yyyy-MM-dd"));
				selectStmt.setString(2, endDate.toString("yyyy-MM-dd"));
				ResultSet result = selectStmt.executeQuery();

				String lastClientID = "";
				while (result.next()) {
					String thisClientID = result.getString("Students.ClientID");
					if (thisClientID.equals(lastClientID))
						continue;

					lastClientID = thisClientID;
					updateRegisteredClassByStudent(result.getInt("ClientID"), result.getString("EventName"));
				}

				result.close();
				selectStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateRegisteredClass: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel model = new StudentNameModel("", "", false);
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, model, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	private void updateRegisteredClassByStudent(int clientID, String eventName) {
		// Update registered (future) class for a student
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Students SET RegisterClass=? WHERE ClientID=?;");
				updateStudentStmt.setString(1, eventName);
				updateStudentStmt.setInt(2, clientID);

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateRegisteredClassByStudent: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false),
						clientID, ": " + e2.getMessage());
				break;
			}
		}
	}

	private void clearRegisteredClasses() {
		// Clear registered classes field for all students (updated daily)
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateStudentStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Students SET RegisterClass = '';");

				updateStudentStmt.executeUpdate();
				updateStudentStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("clearRegisteredClasses: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.STUDENT_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e2.getMessage());
				break;
			}
		}
	}

	private void removeClassFromSchedule(ScheduleModel model) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement deleteClassStmt = sqlDb.dbConnection
						.prepareStatement("DELETE FROM Schedule WHERE ScheduleID=?;");

				// Delete class from schedule
				deleteClassStmt.setInt(1, model.getScheduleID());
				deleteClassStmt.executeUpdate();
				deleteClassStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("removeClassFromSchedule: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.SCHEDULE_DB_ERROR, null, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	private void addClassToSchedule(ScheduleModel importEvent) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement addScheduleStmt = sqlDb.dbConnection.prepareStatement(
						"INSERT INTO Schedule (DayOfWeek, StartTime, Duration, ClassName, NumStudents, "
								+ "Youngest, Oldest, AverageAge, ModuleCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

				int col = 1;
				String className = importEvent.getClassName();

				addScheduleStmt.setInt(col++, importEvent.getDayOfWeek());
				addScheduleStmt.setString(col++, importEvent.getStartTime());
				addScheduleStmt.setInt(col++, importEvent.getDuration());
				if (className.length() >= CLASS_NAME_WIDTH)
					className = className.substring(0, CLASS_NAME_WIDTH);
				addScheduleStmt.setString(col++, className);
				addScheduleStmt.setInt(col++, importEvent.getAttCount());
				addScheduleStmt.setString(col++, importEvent.getYoungest());
				addScheduleStmt.setString(col++, importEvent.getOldest());
				addScheduleStmt.setString(col++, importEvent.getAverageAge());
				addScheduleStmt.setString(col++, importEvent.getModuleCount());

				addScheduleStmt.executeUpdate();
				addScheduleStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("addClassToSchedule: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLIntegrityConstraintViolationException e2) {
				// Schedule data already exists, do nothing
				break;

			} catch (SQLException | NullPointerException e3) {
				e3.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.SCHEDULE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e3.getMessage());
				break;
			}
		}
	}

	private void updateClassInSchedule(ScheduleModel dbEvent, ScheduleModel pike13Event) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateScheduleStmt = sqlDb.dbConnection.prepareStatement(
						"UPDATE Schedule SET NumStudents=?, Youngest=?, Oldest=?, AverageAge=?, ModuleCount=?, "
								+ "Room=?, RoomMismatch=? WHERE ScheduleID=?;");

				int col = 1;
				updateScheduleStmt.setInt(col++, pike13Event.getAttCount());
				updateScheduleStmt.setString(col++, pike13Event.getYoungest());
				updateScheduleStmt.setString(col++, pike13Event.getOldest());
				updateScheduleStmt.setString(col++, pike13Event.getAverageAge());
				updateScheduleStmt.setString(col++, pike13Event.getModuleCount());
				updateScheduleStmt.setString(col++, pike13Event.getRoom());
				updateScheduleStmt.setBoolean(col++, pike13Event.getRoomMismatch());
				updateScheduleStmt.setInt(col, dbEvent.getScheduleID());

				updateScheduleStmt.executeUpdate();
				updateScheduleStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateClassInSchedule: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e3) {
				e3.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.SCHEDULE_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e3.getMessage());
				break;
			}
		}
	}

	/*
	 * ------- Courses Import Database Queries -------
	 */
	public void importCourses(ArrayList<CoursesModel> importList) {
		ArrayList<CoursesModel> dbList = sqlDb.getCourseSchedule("CourseID");
		int dbListIdx = 0;
		int dbListSize = dbList.size();
		int lastCourseIdx = 0;

		Collections.sort(importList);

		for (int i = 0; i < importList.size(); i++) {
			CoursesModel importEvent = importList.get(i);

			// Ignore duplicate courses, only process 1st day
			if (lastCourseIdx == importEvent.getScheduleID())
				continue;
			lastCourseIdx = importEvent.getScheduleID();

			// If at end of DB list, then default operation is insert (1)
			int compare = 1;
			if (dbListIdx < dbListSize)
				compare = dbList.get(dbListIdx).compareTo(importEvent);

			if (compare == 0) {
				// All data matches
				dbListIdx++;

			} else if (compare == 1) {
				// Insert new event into DB
				addCourseToSchedule(importEvent);

			} else if (compare == 2) {
				// Same record but content has changed, so update
				updateCourse(importEvent);
				dbListIdx++;

			} else {
				// Extra event(s) in database, so delete them
				while (compare < 0) {
					removeCourseFromSchedule(dbList.get(dbListIdx));
					dbListIdx++;

					compare = 1;
					if (dbListIdx < dbListSize)
						// Continue to compare until dbList catches up
						compare = dbList.get(dbListIdx).compareTo(importEvent);
				}
				// One final check to get in sync with importEvent
				if (compare == 0) {
					// Match, so continue incrementing through list
					dbListIdx++;

				} else if (compare == 1) {
					// Insert new event into DB
					addCourseToSchedule(importEvent);

				} else if (compare == 2) {
					// Same record but content has changed, so update
					updateCourse(importEvent);
					dbListIdx++;
				}
			}
		}

		// Delete extra entries at end of dbList
		while (dbListIdx < dbListSize) {
			removeCourseFromSchedule(dbList.get(dbListIdx));
			dbListIdx++;
		}
	}

	private void addCourseToSchedule(CoursesModel courseEvent) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement addCourseStmt = sqlDb.dbConnection
						.prepareStatement("INSERT INTO Courses (CourseID, EventName, Enrolled) " + "VALUES (?, ?, ?);");

				int col = 1;
				addCourseStmt.setInt(col++, courseEvent.getScheduleID());
				addCourseStmt.setString(col++, courseEvent.getEventName().trim());
				addCourseStmt.setInt(col, courseEvent.getEnrollment());

				addCourseStmt.executeUpdate();
				addCourseStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("addCourseToSchedule: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLIntegrityConstraintViolationException e2) {
				// Schedule data already exists, do nothing
				break;

			} catch (SQLException | NullPointerException e3) {
				e3.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.COURSES_DB_ERROR, new StudentNameModel("", "", false), 0,
						": " + e3.getMessage());
				break;
			}
		}
	}

	private void updateCourse(CoursesModel course) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement updateCourseStmt = sqlDb.dbConnection
						.prepareStatement("UPDATE Courses SET EventName=?, Enrolled=? WHERE CourseID=?;");

				int col = 1;
				updateCourseStmt.setString(col++, course.getEventName().trim());
				updateCourseStmt.setInt(col++, course.getEnrollment());
				updateCourseStmt.setInt(col, course.getScheduleID());

				updateCourseStmt.executeUpdate();
				updateCourseStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("updateCourse: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				StudentNameModel studentModel = new StudentNameModel("", "", true);
				MySqlDbLogging.insertLogData(LogDataModel.COURSES_DB_ERROR, studentModel, 0,
						" for " + course.getEventName().trim() + ": " + e2.getMessage());
				break;
			}
		}
	}

	private void removeCourseFromSchedule(CoursesModel course) {
		for (int i = 0; i < 2; i++) {
			try {
				// If Database no longer connected, the exception code will re-connect
				PreparedStatement deleteClassStmt = sqlDb.dbConnection
						.prepareStatement("DELETE FROM Courses WHERE CourseID=?;");

				// Delete class from schedule
				deleteClassStmt.setInt(1, course.getScheduleID());
				deleteClassStmt.executeUpdate();
				deleteClassStmt.close();
				break;

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("removeCourseFromSchedule: " + e1.getMessage() + ", " + e1);
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e2) {
				e2.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.COURSES_DB_ERROR, null, 0, ": " + e2.getMessage());
				break;
			}
		}
	}

	/*
	 * ------- Github utilities -------
	 */
	private StudentModel findGithubUser (ArrayList<StudentModel> students, String gitUser)
	{
		// Find the Git User name in the student list
		for (StudentModel s : students) {
			if (s.getGithubName() != null && gitUser.equals(s.getGithubName().toLowerCase().trim()))
				return s;
		}
		return null;
	}
	
	private void clearAllPendGithubStatus ()
	{
		// Clear all the missed Git and Git state in the Pending Github table
		PreparedStatement clrPendingGitStmt;
		for (int i = 0; i < 2; i++) {
			try {
				clrPendingGitStmt = sqlDb.dbConnection.prepareStatement("UPDATE PendingGithub SET GotGit='', Status='';");
				clrPendingGitStmt.executeUpdate();
				clrPendingGitStmt.close();

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("clearAllGithubStatus: " + e1.getMessage());
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.GITHUB_DB_ERROR, new StudentNameModel("", "", false), 0,
						" clearing git status: " + e.getMessage());
				break;
			}
		}
	}
	
	private void setMissingGithub (PendingGithubModel git)
	{
		// Set the GotGit field in the given Pending Github record
		PreparedStatement setMissingGitStmt;
		for (int i = 0; i < 2; i++) {
			try {
				setMissingGitStmt = sqlDb.dbConnection.prepareStatement("UPDATE PendingGithub SET GotGit='?' WHERE PrimaryID=?;");
				setMissingGitStmt.setInt(1, git.getPrimaryID());
				setMissingGitStmt.executeUpdate();
				setMissingGitStmt.close();

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("setMissingGithub: " + e1.getMessage());
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.GITHUB_DB_ERROR, new StudentNameModel("", "", false), 0,
						" updating missing git in record: " + e.getMessage());
				break;
			}
		}
	}
	
	private void setGithubStatus (PendingGithubModel git, String status)
	{
		// Set the State field in the given Pending Github record
		PreparedStatement setGitStatusStmt;
		for (int i = 0; i < 2; i++) {
			try {
				setGitStatusStmt = sqlDb.dbConnection.prepareStatement("UPDATE PendingGithub SET Status=? WHERE PrimaryID=?;");
				setGitStatusStmt.setString(1, status);
				setGitStatusStmt.setInt(2, git.getPrimaryID());
				setGitStatusStmt.executeUpdate();
				setGitStatusStmt.close();

			} catch (CommunicationsException | MySQLNonTransientConnectionException e1) {
				if (i == 0) {
					// First attempt to re-connect
					System.out.println("setGithubStatus: " + e1.getMessage());
					sqlDb.connectDatabase();
				}

			} catch (SQLException | NullPointerException e) {
				e.printStackTrace();
				MySqlDbLogging.insertLogData(LogDataModel.GITHUB_DB_ERROR, new StudentNameModel("", "", false), 0,
						" updating git status in record: " + e.getMessage());
				break;
			}
		}
	}
	
	/*
	 * ------- Miscellaneous utilities -------
	 */
	private String parseTeacherNames(String origTeachers) {
		if (origTeachers == null || origTeachers.equals(""))
			return "";

		String teachers = "";
		String[] values = origTeachers.split("\\s*,\\s*");
		for (int i = 0; i < values.length; i++) {
			String valueLC = values[i].toLowerCase();
			if (values[i].startsWith("TA-") || valueLC.startsWith("trainee-") || valueLC.startsWith("open lab") || valueLC.startsWith("sub teacher")
					|| valueLC.startsWith("padres game") || valueLC.startsWith("make-up")
					|| valueLC.startsWith("intro to java") || valueLC.startsWith("league admin") || valueLC.startsWith("coding camp")
					|| valueLC.startsWith("summer prog") || valueLC.startsWith("need assist") || valueLC.startsWith("needs assist")
					|| valueLC.startsWith("league workshop") || valueLC.startsWith("accepting student"))
				continue;

			if (!teachers.equals(""))
				teachers += ", ";
			teachers += values[i];
		}
		
		// No teachers for this class; use the TA's instead
		if (teachers.equals("")) {
			for (int i = 0; i < values.length; i++) {
				if (values[i].startsWith("TA-")) {
					if (!teachers.equals(""))
						teachers += ", ";
					teachers += values[i];
				}
			}
		}
		return teachers;
	}

	private int getClientIdxInStudentList(ArrayList<StudentModel> list, int clientID) {
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).getClientID() == clientID)
				return i;
		}
		return -1;
	}
}
