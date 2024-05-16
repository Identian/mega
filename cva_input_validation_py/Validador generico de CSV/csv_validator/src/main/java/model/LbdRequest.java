package model;

public class LbdRequest {

	private String s3CsvPath;
	private String s3SchemaPath;
	private String s3SchemaDescriptionPath;
	private String s3SchemaBucket;
	private String s3CsvBucket;
	
	public String getS3CsvPath() {
		return s3CsvPath;
	}
	public void setS3CsvPath(String s3CsvPath) {
		this.s3CsvPath = s3CsvPath;
	}
	public String getS3SchemaPath() {
		return s3SchemaPath;
	}
	public void setS3SchemaPath(String s3SchemaPath) {
		this.s3SchemaPath = s3SchemaPath;
	}
	public String getS3SchemaDescriptionPath() {
		return s3SchemaDescriptionPath;
	}
	public void setS3SchemaDescriptionPath(String s3SchemaDescriptionPath) {
		this.s3SchemaDescriptionPath = s3SchemaDescriptionPath;
	}
	public String getS3SchemaBucket() {
		return s3SchemaBucket;
	}
	public void setS3SchemaBucket(String s3SchemaBucket) {
		this.s3SchemaBucket = s3SchemaBucket;
	}
	public String getS3CsvBucket() {
		return s3CsvBucket;
	}
	public void setS3CsvBucket(String s3CsvBucket) {
		this.s3CsvBucket = s3CsvBucket;
	}
	@Override
	public String toString() {
		return "LbdRequest [s3CsvPath=" + s3CsvPath + ", s3SchemaPath=" + s3SchemaPath + ", s3SchemaDescriptionPath="
				+ s3SchemaDescriptionPath + ", s3SchemaBucket=" + s3SchemaBucket + ", s3CsvBucket=" + s3CsvBucket + "]";
	}
	
}
