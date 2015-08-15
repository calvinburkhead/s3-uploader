package org.nwmsrocks.s3uploader;

import java.io.Serializable;

public class S3Object implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4688624088876599624L;
	private String path;
	private String prefix;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	public S3Object(String path)
	{
		this.path = path;
		this.prefix = "";
	}

	public S3Object(String prefix, String path) {
		this.prefix = prefix;
		this.path = path;
	}
	
	@Override
	public String toString(){
		return getPath();
	}

}
