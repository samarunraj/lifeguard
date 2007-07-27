
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

import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfWriter;

import com.directthought.lifeguard.AbstractBaseService;
import com.directthought.lifeguard.AbstractBaseService.MetaFile;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.ServiceConfig;

/**
 * This service converts an incomming text file to pdf, in a very basic way.
 *
 */
public class TextToPdfService extends AbstractBaseService {
	private static Log logger = LogFactory.getLog(TextToPdfService.class);

	public TextToPdfService(ServiceConfig config) {
		super(config);
	}

	public String getServiceName() {
		return "TextToPdf";
	}

	public List<MetaFile> executeService(File inputFile, WorkRequest request) {
		Document document = new Document();
		String outFileName = inputFile.getName();
		int idx = outFileName.lastIndexOf('.');
		outFileName = ((idx==-1)?outFileName:outFileName.substring(0, idx-1))+".pdf";
		try {
			PdfWriter.getInstance(document, new FileOutputStream(outFileName));

			document.open();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
											new FileInputStream(inputFile)));
			Paragraph para = new Paragraph();
			String line = rdr.readLine();
			while (line!=null) {
				para.add(line);
				line = rdr.readLine();
			}
			document.add(para);
		} catch (DocumentException ex) {
			logger.error(ex.getMessage(), ex);
		} catch (IOException ex) {
			logger.error(ex.getMessage(), ex);
		}
		document.close();

		ArrayList ret = new ArrayList();
		ret.add(new MetaFile(new File(outFileName), "application/pdf"));
		return ret;
	}
}
