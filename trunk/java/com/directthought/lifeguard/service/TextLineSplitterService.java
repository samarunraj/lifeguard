
package com.directthought.lifeguard.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.directthought.lifeguard.AbstractBaseService;
import com.directthought.lifeguard.MetaFile;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.ServiceConfig;

/**
 * This service converts an incomming text file to pdf, in a very basic way.
 *
 */
public class TextLineSplitterService extends AbstractBaseService {
	private static Log logger = LogFactory.getLog(TextLineSplitterService.class);

	public TextLineSplitterService(ServiceConfig config, String accessId, String secretKey, String queuePrefix) {
		super(config, accessId, secretKey, queuePrefix);
	}

	public List<MetaFile> executeService(File inputFile, WorkRequest request) {
		String outFileName = inputFile.getName();
		int idx = outFileName.lastIndexOf('.');
		outFileName = ((idx==-1)?outFileName:outFileName.substring(0, idx-1))+".txt";
		try {
			FileOutputStream fOut = new FileOutputStream(outFileName);
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
											new FileInputStream(inputFile)));
			String line = rdr.readLine();
			while (line!=null) {
				StringTokenizer tok = new StringTokenizer(line, " \n\t", false);
				while (tok.hasMoreElements()) {
					fOut.write(tok.nextToken().getBytes());
					fOut.write('\n');
				}
				line = rdr.readLine();
			}
			rdr.close();
			fOut.close();
		} catch (IOException ex) {
			logger.error(ex.getMessage(), ex);
		}

		ArrayList ret = new ArrayList();
		ret.add(new MetaFile(new File(outFileName), "text/plain"));
		return ret;
	}
}
