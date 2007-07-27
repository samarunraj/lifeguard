
package com.directthought.lifeguard.util;

import java.io.InputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {
	private static final String pseudo[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8",
							"9", "A", "B", "C", "D", "E", "F"};

	public static String md5Sum(String msg) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(msg.getBytes(), 0, msg.length());
		return new String(byteArrayToHexString(md.digest()));
	}

	public static String md5Sum(InputStream iStr) throws IOException, NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte [] buf = new byte[64*1024];
		int count = iStr.read(buf);
		while (count > 0) {
			md.update(buf, 0, count);
			count = iStr.read(buf);
		}
		iStr.close();
		return new String(byteArrayToHexString(md.digest()));
	}

	/**
	 * This code copied from DevX.com - posted by Jeff Boyle
	 * Convert a byte[] array to readable string format. This makes the "hex" readable!
	 * @return result String buffer in String format 
	 * @param in byte[] buffer to convert to string format
	 */
	static String byteArrayToHexString(byte in[]) {
		byte ch = 0x00;
		int i = 0; 

		if (in == null || in.length <= 0)
			return null;

		StringBuffer out = new StringBuffer(in.length * 2);
		while (i < in.length) {
			ch = (byte) (in[i] & 0xF0); // Strip off high nibble
			ch = (byte) (ch >>> 4); // shift the bits down
			ch = (byte) (ch & 0x0F);    // must do this is high order bit is on!
			out.append(pseudo[ (int) ch]); // convert the nibble to a String Character
			ch = (byte) (in[i] & 0x0F); // Strip off low nibble 
			out.append(pseudo[ (int) ch]); // convert the nibble to a String Character
			i++;
		}
		String rslt = new String(out);
		return rslt;
	} 
}
