package com.vholdr.aws;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.directthought.lifeguard.AbstractBaseService;
import com.directthought.lifeguard.ServiceException;
import com.directthought.lifeguard.jaxb.ParamType;
import com.directthought.lifeguard.jaxb.ServiceConfig;
import com.directthought.lifeguard.jaxb.WorkRequest;

public class VideoConversionService extends AbstractBaseService {
	//NOTE: We use apache commons logging here (rather than log4j) because
	//the rest of the lifeguard app uses apache commons and this way we don't
	//have to package log4j.log into our ami for this one class.
	private static Log logger = LogFactory.getLog(VideoConversionService.class);

	public VideoConversionService(ServiceConfig config, String accessId,
			String secretKey, String queuePrefix) {
		super(config, accessId, secretKey, queuePrefix);
	}

	/**
	 * Convert the video to flash format using hard-wired parameters.
	 * We'll move those out to the work request at some point.
	 * Also make a thumbnail image of the video.
	 */
	public List<MetaFile> executeService(File inputFile, WorkRequest request) 
	  throws ServiceException
	{	
		List<MetaFile> results = new ArrayList<MetaFile>();
		String inputFileName;
		String videoFileName;
		String thumbFileName;

		inputFileName = inputFile.getAbsolutePath();
		videoFileName = inputFile.getAbsolutePath();
		thumbFileName = inputFile.getAbsolutePath();
	
		logger.debug("input file name is "+inputFileName);
		int lastDot = inputFileName.lastIndexOf('.');
		videoFileName = (lastDot == -1) ? videoFileName+".flv" : videoFileName.substring(0,lastDot)+".flv";
		thumbFileName = (lastDot == -1) ? thumbFileName+".jpg" : thumbFileName.substring(0,lastDot)+".jpg";
		logger.debug("output video file name is "+videoFileName);
		logger.debug("output thumb file name is "+thumbFileName);
		
		//Get the ffmpeg commands for converting video and making thumbnails
		String videoConversionCmd = null;
		String thumbnailCmd = null;
		long videoTimeout = 120000; //default to 2 minutes
		long thumbnailTimeout = 900000; //default to 15 minutes
		for (ParamType p : request.getParams()) {
			if ("VideoConversionCommand".equals(p.getName())) {
				videoConversionCmd = p.getValue();
			} else if ("ThumbnailCreationCommand".equals(p.getName())) {
				thumbnailCmd = p.getValue();
			} else if ("VideoTimeoutMilliseconds".equals(p.getName())) {
				videoTimeout = Long.parseLong(p.getValue());
			} else if ("ThumbnailTimeoutMilliseconds".equals(p.getName())) {
				thumbnailTimeout = Long.parseLong(p.getValue());
			}
		}
		
		try {
			ConversionThread convertVideo = new ConversionThread(videoConversionCmd, inputFileName, videoFileName);
			convertVideo.start();
			logger.debug("started video conversion process");
			//wait for the given timeout for the conversion to finish
			convertVideo.join(videoTimeout);
			int result = convertVideo.getConversionResult();
			logger.debug("video conversion process finished");
			if (result == 0) {
				results.add(new MetaFile(new File(videoFileName), "video/x-flv"));
			} else {
				throw new ServiceException("conversion of "+videoFileName+" failed or timed out");
			}
			
			ConversionThread makeThumb = new ConversionThread(thumbnailCmd, inputFileName, thumbFileName);
			makeThumb.start();
			logger.debug("started thumbnail generation");
			//wait for the given timeout for the conversion to finish
			makeThumb.join(thumbnailTimeout);
			result = makeThumb.getConversionResult();
			logger.debug("finished thumbnail generation");
			if (result == 0) {
				results.add(new MetaFile(new File(thumbFileName), "image/jpeg"));
			} else {
				throw new ServiceException("thumbnail creation for "+videoFileName+" failed or timed out");
			}
		} catch (InterruptedException e) {
			throw new ServiceException("Processing of video was interrupted", e);
		} catch (IllegalArgumentException e) {
			throw new ServiceException("Illegal arguments passed for video processing", e);
		}
		return results;
	}
	
	/**
	 * Utility class to create a thread that will start an external ffmpeg process
	 * and allow you to retrieve the return code of that process. The default
	 * return code (returned if the process fails to start) is -1.
	 * @author noah
	 *
	 */
	private class ConversionThread extends Thread {
		private ProcessBuilder conversionProcess;
		private int conversionResult;
		
		/**
		 * Create a new thread that can be used to run a video conversion process.
		 * 
		 * @param command The command to run. Use '%i' as a placeholder for the input file parameter (may be used more than once) and '%o' as a placeholder for the output file parameter.
		 * @param inputFile The input file the command will operate on
		 * @param outputFile The output file the command will write to
		 * @throws IllegalArgumentException If any of the required parameters are missing
		 */
		public ConversionThread(String command, String inputFile, String outputFile) throws IllegalArgumentException {
			conversionResult = -1;
			if (command == null) throw new IllegalArgumentException("No command provided for video conversion");
			if (inputFile == null) throw new IllegalArgumentException("No input file name provided for video conversion");
			if (outputFile == null) throw new IllegalArgumentException("No output file name provided for video conversion");
			String fullCommand = command.replaceAll("%i", inputFile);
			fullCommand = fullCommand.replaceAll("%o", outputFile);
			List<String> cmdArgs = Arrays.asList(fullCommand.split(" "));
			conversionProcess = new ProcessBuilder(cmdArgs);
		}
		
		public void run() {
			Process p = null;
			try {
				p = conversionProcess.start();
				BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				String line = null;
				while ((line=stderr.readLine()) != null) {
					logger.debug(line);
				}
				conversionResult = p.waitFor();
			} catch (IOException e) {
				logger.error(e);
				conversionResult = -1;
			} catch (InterruptedException e) {
				logger.error(e);
				conversionResult = -1;
			}
		}
		
		
		public int getConversionResult() {
			return conversionResult;
		}
	}
}
