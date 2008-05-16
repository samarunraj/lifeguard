
package com.directthought.lifeguard.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
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
 * This service runs ghostscript on the input file to produce raster tiff output.
 *
 */
public class GhostscriptService extends AbstractBaseService {
	private static Log logger = LogFactory.getLog(GhostscriptService.class);

	private final static String MONO_DEV = "tiffg4";
	private final static String GRAY_DEV = "tiffgray";
	private final static String RGB_DEV = "tiff24nc";
	private final static String CMYK_DEV = "tiff32nc";

	private final static String RUN_CMD = "./run_ghostscript ";

	private int dpi = 600;

	public GhostscriptService(ServiceConfig config, String accessId, String secretKey, String queuePrefix) {
		super(config, accessId, secretKey, queuePrefix);
	}

	public List<MetaFile> executeService(File inputFile, WorkRequest request) throws ServiceException {
		String outFileName = inputFile.getName();
		int idx = outFileName.lastIndexOf('.');
		outFileName = ((idx==-1)?outFileName:outFileName.substring(0, idx-1));
		String device = MONO_DEV;
		for (ParamType p : request.getParams()) {
			String param = p.getName();
			String value = p.getValue();
			if (param.equals("color")) {
				if (value.equals("MONO")) {
					device = MONO_DEV;
				}
				else if (value.equals("GRAY")) {
					device = GRAY_DEV;
				}
				else if (value.equals("RGB")) {
					device = RGB_DEV;
				}
				else if (value.equals("CMYK")) {
					device = CMYK_DEV;
				}
			}
			else if (param.equals("dpi")) {
				try {
					dpi = Integer.parseInt(value);
				} catch (NumberFormatException ex) {
					logger.error("Couldn't parse DPI, using default", ex);
				}
			}
		}
		try {
			File workingDir = new File("/home/lifeguard");
			logger.info("Going to run : "+RUN_CMD+inputFile.getPath());
			Process proc = Runtime.getRuntime().exec(
							RUN_CMD+inputFile.getPath()+" "+device+" "+dpi,
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
				throw new ServiceException("ghostscript script failed.");
			}
		} catch (InterruptedException ex) {
			logger.error(ex.getMessage(), ex);
		} catch (IOException ex) {
			logger.error(ex.getMessage(), ex);
		}

		ArrayList ret = new ArrayList();
		final String tmp = outFileName;
		File [] outFiles = inputFile.getParentFile().listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return (name.startsWith(tmp) && name.endsWith("tif"));
				}
			});
		for (File f : outFiles) {
			ret.add(new MetaFile(f, "image/tiff"));
		}
		return ret;
	}
}
