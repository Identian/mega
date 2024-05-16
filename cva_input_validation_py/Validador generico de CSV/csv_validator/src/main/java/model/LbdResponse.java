package model;

public class LbdResponse {
	
	private int statusCode;
	private String body;
	private boolean isValidCsv;
	private String validStruct;
	
	public LbdResponse(int statusCode, String body, boolean isValidCsv, String validStruct) {
		super();
		this.statusCode = statusCode;
		this.body = body;
		this.isValidCsv = isValidCsv;
		this.validStruct = validStruct;
	}
	
	public String getValidStruct() {
		return validStruct;
	}
	public void setValidStruct(String validStruct) {
		this.validStruct = validStruct;
	}
	public int getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public boolean isValidCsv() {
		return isValidCsv;
	}

	public void setValidCsv(boolean isValidCsv) {
		this.isValidCsv = isValidCsv;
	}
	
}
