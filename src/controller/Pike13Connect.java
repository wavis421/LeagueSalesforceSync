package controller;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import model.LogDataModel;
import model.MySqlDbLogging;
import model.StudentNameModel;

public class Pike13Connect {
	private final String USER_AGENT = "Mozilla/5.0";
	private String pike13Token;
	
	public Pike13Connect(String pike13Token) {
		this.pike13Token = pike13Token;
	}

	private HttpURLConnection connectUrl(String endPoint, boolean coreApi) {
		HttpURLConnection conn = null;
		String urlString;
		
		// Typically the 'reporting' API is used; occasionally fields are not available
		// in the reporting API so the core API must be used instead.
		if (coreApi)
			urlString = "https://jtl.pike13.com/api/v2/desk/" + endPoint;
		else
			urlString = "https://jtl.pike13.com/desk/api/v3/reports/" + endPoint + "/queries";

		try {
			// Get URL connection with authorization
			URL url = new URL(urlString);
			conn = (HttpURLConnection) url.openConnection();
			if (conn == null) {
				MySqlDbLogging.insertLogData(LogDataModel.PIKE13_CONNECTION_ERROR, new StudentNameModel("", "", false), 0,
						": Failed to open connection for endpoint '" + endPoint + "'");
				return null;
			}
			String basicAuth = "Bearer " + pike13Token;
			conn.setRequestProperty("Authorization", basicAuth);
			conn.setRequestProperty("User-Agent", USER_AGENT);

			if (coreApi)
				conn.setRequestMethod("GET");
			else
				conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-type", "application/vnd.api+json; charset=UTF-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			return conn;

		} catch (Exception e) {
			MySqlDbLogging.insertLogData(LogDataModel.PIKE13_CONNECTION_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
			e.printStackTrace();
		
			if (conn != null)
				conn.disconnect();
		}
		return null;
	}

	public HttpURLConnection sendQueryToUrl(String connName, String getCommand, boolean coreApi) {
		try {
			// If necessary, try twice to send query
			for (int i = 0; i < 2; i++) {
				// Get URL connection with authorization
				HttpURLConnection conn = connectUrl(connName, coreApi);
				if (conn == null)
					continue;

				// Send the query
				if (!coreApi) {
					OutputStream outputStream = conn.getOutputStream();
					outputStream.write(getCommand.getBytes("UTF-8"));
					outputStream.flush();
					outputStream.close();
				}

				// Check result
				int responseCode = conn.getResponseCode();
				if (responseCode == HttpURLConnection.HTTP_OK)
					return conn;
				else {
					MySqlDbLogging.insertLogData(LogDataModel.PIKE13_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
							" " + responseCode + " for '" + connName + "' (attempt #" + (i + 1) + "): " + conn.getResponseMessage());
					conn.disconnect();
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
			MySqlDbLogging.insertLogData(LogDataModel.PIKE13_IMPORT_ERROR, new StudentNameModel("", "", false), 0, 
					": " + e.getMessage());
		}

		return null;
	}

	public JsonObject readInputStream(HttpURLConnection conn) {
		try {
			// Get input stream and read data
			InputStream inputStream = conn.getInputStream();
			JsonReader repoReader = Json.createReader(inputStream);
			JsonObject object = ((JsonObject) repoReader.read()).getJsonObject("data").getJsonObject("attributes");

			repoReader.close();
			inputStream.close();
			return object;

		} catch (IOException e) {
			e.printStackTrace();
			MySqlDbLogging.insertLogData(LogDataModel.PIKE13_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
					": " + e.getMessage());
		}
		return null;
	}
	
	public JsonObject readCoreInputStream(HttpURLConnection conn) {
		try {
			// Get input stream and read data
			InputStream inputStream = conn.getInputStream();
			JsonReader repoReader = Json.createReader(inputStream);
			JsonObject object = ((JsonObject) repoReader.read());

			repoReader.close();
			inputStream.close();
			return object;

		} catch (IOException e) {
			e.printStackTrace();
			MySqlDbLogging.insertLogData(LogDataModel.PIKE13_IMPORT_ERROR, new StudentNameModel("", "", false), 0,
					" for Core API: " + e.getMessage());
		}
		return null;
	}

	public String stripQuotes(String fieldData) {
		// Strip off quotes around field string
		if (fieldData.equals("\"\"") || fieldData.startsWith("null"))
			return "";
		else
			return fieldData.substring(1, fieldData.length() - 1);
	}
}
