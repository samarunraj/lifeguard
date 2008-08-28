
package com.directthought.lifeguard;

import java.io.File;

/**
 * This is a wrapper class for a file, plus some other information
 */
public class MetaFile {
	public File file;
	public String mimeType;
	public String key;

	public MetaFile(File file, String mimeType) {
		this(file, mimeType, null);
	}

	public MetaFile(File file, String mimeType, String key) {
		this.file = file;
		this.mimeType = mimeType;
		this.key = key;
	}
}

