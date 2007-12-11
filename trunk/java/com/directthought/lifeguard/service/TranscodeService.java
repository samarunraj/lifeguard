
package com.directthought.lifeguard.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.directthought.lifeguard.AbstractBaseService;
import com.directthought.lifeguard.AbstractBaseService.MetaFile;
import com.directthought.lifeguard.ServiceException;
import com.directthought.lifeguard.jaxb.ParamType;
import com.directthought.lifeguard.jaxb.ServiceConfig;
import com.directthought.lifeguard.jaxb.WorkRequest;

/**
 * This service transcodes one video file to another.
 *
 */
public class TranscodeService extends AbstractBaseService {
	private static Log logger = LogFactory.getLog(TranscodeService.class);

	// ffmpeg -y -i %s -f mov -r 29.97 -b 1200kb -mbd 2 -flags +4mv+trell -aic 2 -cmp 2 -subcmp 2 -ar 48000 -ab 192 -s 320x240 -vcodec mpeg4 -acodec aac %s
	private final static String RUN_CMD = "./ffmpeg -y -i ";

	public TranscodeService(ServiceConfig config, String accessId, String secretKey, String queuePrefix) {
		super(config, accessId, secretKey, queuePrefix);
	}

	public List<MetaFile> executeService(File inputFile, WorkRequest request) throws ServiceException {
		String outFileName = inputFile.getName();
		int idx = outFileName.lastIndexOf('.');
		outFileName = ((idx==-1)?outFileName:outFileName.substring(0, idx-1))+".avi";
		StringBuilder args = new StringBuilder();
		for (ParamType p : request.getParams()) {
			String param = p.getName();
			if (param.startsWith("xcode.")) {
				args.append(" -");
				args.append(param.substring(6));
				args.append(" ");
				args.append(p.getValue());
			}
		}
		try {
			File workingDir = new File("/home/lifeguard");
			logger.info("Going to run : "+RUN_CMD+inputFile.getPath()+args.toString()+" "+inputFile.getParentFile().getPath()+"/"+outFileName);
			Process proc = Runtime.getRuntime().exec(
							RUN_CMD+inputFile.getPath()+args.toString()+" "+inputFile.getParentFile().getPath()+"/"+outFileName,
							null,
							workingDir);
	// for lots of fun debug... uncomment the lines below
			BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String line = null;
			while ((line=stderr.readLine()) != null) {
				logger.info(line);
			}
			int result = proc.waitFor();
			if (result != 0) {
				logger.error("The shell command had an error.");
				throw new ServiceException("ffmpeg script failed.");
			}
		} catch (InterruptedException ex) {
			logger.error(ex.getMessage(), ex);
		} catch (IOException ex) {
			logger.error(ex.getMessage(), ex);
		}

		ArrayList ret = new ArrayList();
		ret.add(new MetaFile(new File(outFileName), "text/plain"));
		return ret;
	}
}
