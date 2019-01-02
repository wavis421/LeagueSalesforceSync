package model;

//Model for importing Staff Hours data into SalesForce
public class SalesForceStaffHoursModel {
	private String clientID;
	private String fullName;
	private String serviceName;
	private String serviceDate;
	private String serviceCategory;
	private String serviceTime;
	private double hours;
	private String location;
	private double completed;
	private double noShow;
	private double lateCanceled;
	private String eventName;
	private String scheduleID;

	public SalesForceStaffHoursModel(String clientID, String fullName, String serviceName, String serviceDate,
			String serviceTime, double hours, String location, double completed, double noShow, double lateCanceled,
			String eventName, String scheduleID, String serviceCategory) {

		this.clientID = clientID;
		this.fullName = fullName;
		this.serviceName = serviceName;
		this.serviceDate = serviceDate;
		this.serviceTime = serviceTime;
		this.hours = hours;
		this.location = location;
		this.completed = completed;
		this.noShow = noShow;
		this.lateCanceled = lateCanceled;
		this.eventName = eventName;
		this.scheduleID = scheduleID;
		this.serviceCategory = serviceCategory;
	}

	@Override
	public String toString() {
		return clientID + ", " + fullName + " for '" + eventName + "/" + serviceName + "' at " + serviceDate + " "
				+ serviceTime + " (" + hours + " hours), " + location + ": " + completed + "/" + noShow + "/"
				+ lateCanceled + ", " + scheduleID;
	}

	public String getClientID() {
		return clientID;
	}

	public String getFullName() {
		return fullName;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getServiceDate() {
		return serviceDate;
	}

	public String getServiceTime() {
		return serviceTime;
	}

	public double getHours() {
		return hours;
	}

	public String getLocation() {
		return location;
	}

	public double getCompleted() {
		return completed;
	}

	public double getNoShow() {
		return noShow;
	}

	public double getLateCanceled() {
		return lateCanceled;
	}

	public String getEventName() {
		return eventName;
	}

	public String getScheduleID() {
		return scheduleID;
	}

	public String getServiceCategory() {
		return serviceCategory;
	}
}
