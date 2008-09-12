
package com.directthought.lifeguard.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class Copier {
	public static void copyStreams(InputStream iStr, OutputStream oStr) throws IOException {
		byte [] buf = new byte[64*1024];	// 64k i/o buffer
		int count = iStr.read(buf);
		while (count != -1) {
			if (count > 0) {
				oStr.write(buf, 0, count);
			}
			count = iStr.read(buf);
		}
		oStr.flush();
		oStr.close();
		iStr.close();
	}
}
