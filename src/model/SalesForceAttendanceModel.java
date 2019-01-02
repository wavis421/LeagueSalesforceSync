package model;

//Model for importing data into SalesForce
public class SalesForceAttendanceModel {
	private String clientID;
	private String fullName;
	private String serviceDate;
	private String serviceTime;
	private String eventName;
	private String eventType;
	private String serviceName;
	private String status;
	private String visitID;
	private String scheduleID;
	private String location;
	private String staff;

	public SalesForceAttendanceModel(String clientID, String fullName, String serviceDate, String serviceTime,
			String eventName, String eventType, String serviceName, String status, String visitID, String scheduleID,
			String location, String staff) {

		this.clientID = clientID;
		this.fullName = fullName;
		this.serviceDate = serviceDate;
		this.serviceTime = serviceTime;
		this.eventName = eventName;
		this.eventType = eventType;
		this.serviceName = serviceName;
		this.status = status;
		this.visitID = visitID;
		this.scheduleID = scheduleID;
		this.location = location;
		this.staff = staff;
	}

	public String getClientID() {
		return clientID;
	}

	public String getFullName() {
		return fullName;
	}

	public String getServiceDate() {
		return serviceDate;
	}

	public String getServiceTime() {
		return serviceTime;
	}

	public String getEventName() {
		return eventName;
	}

	public String getEventType() {
		return eventType;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getStatus() {
		return status;
	}

	public String getVisitID() {
		return visitID;
	}

	public String getScheduleID() {
		return scheduleID;
	}

	public String getLocation() {
		return location;
	}

	public String getStaff() {
		return staff;
	}
}
