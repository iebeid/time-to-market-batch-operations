package com.xpress.ttm.control;

import java.io.File;
import java.io.IOException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.xpress.ttm.beans.ActionMapping;
import com.xpress.ttm.beans.TtmActions;
import com.xpress.ttm.utils.TTMFileFilter;


public class TTMIFHandler {

	/*
	 * The INPUT directory
	 */
	private static File inputDirectory;
	/*
	 * The TTM TEMP directory
	 */
	private static File tempDirectory;
	/*
	 * The TTM PROCESSED directory
	 */
	private static File processedDirectory;
	/*
	 * The TTM REJECTED directory
	 */
	private static File rejectedDirectory;
	/*
	 * The TTM tool root directory
	 */
	private static File rootDirectory;
	/*
	 * The WORK directory
	 */
	private static File workDirectory;
	
	private File inputFile ;

	private static final Logger ttmIFHandlerLogger = Logger
			.getLogger(TTMIFHandler.class);
		
	// Parsing the catalog file "action-mapping.xml" 
	/*
	 *  This method will return list of all Actions the tool can handle. This action will 
	 *  contain the action procedure and the action handler    
	 *  
	 */
	public ActionMapping configureActionMapping() {

		File actionMappingFile = new File(rootDirectory.getAbsolutePath() + "/action-mapping.xml");

		JAXBContext jaxbContext;
		Unmarshaller unmarshaller;
		ActionMapping actionMapping = null;

		Schema schema = null;
		SchemaFactory factory = SchemaFactory
				.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Validator validator;
		Source source = null ;
		
		if (!actionMappingFile.exists()) {
			ttmIFHandlerLogger.error(" \"action-mapping.xml\" is not found");
		} else {		
			try {
				source = new StreamSource(TTMIFHandler.class.getResourceAsStream("TtmMappingSchema.xsd"));
				if (source != null) {									
					schema = factory.newSchema(source);
					validator = schema.newValidator();
					try {						
						validator.validate(new StreamSource(actionMappingFile));
						validator.reset();
						try {
							jaxbContext = JAXBContext.newInstance("com.xpress.ttm.beans");
							unmarshaller = jaxbContext.createUnmarshaller();
							actionMapping = (ActionMapping) unmarshaller
									.unmarshal(actionMappingFile);
						} catch (JAXBException e) {
							ttmIFHandlerLogger.error("Failed to parse \"action-mapping.xml\" ", e);
						}
					} catch (SAXException e) {
						ttmIFHandlerLogger.error("Failed to parse: \""
								+ actionMappingFile.getName()+ "\" invalid mapping file" + e.getMessage());
					} catch (IOException e1) {
						ttmIFHandlerLogger.error("Failed to load: \""
								+ actionMappingFile.getName()+ "\"  file does not exist");
					}
				} else {
					actionMapping = null;
				}
			} catch (SAXException e) {
				ttmIFHandlerLogger.error("Failed to parse: \""
					+ actionMappingFile.getName() + "\" invalid schema file" + e.getMessage());
			}
		}		

		return actionMapping;
	}

	
	@SuppressWarnings("unchecked")
	/*
	 *  This function
	 */
	public JAXBElement<TtmActions> confgiureActions() throws ArrayIndexOutOfBoundsException{		

		// Action
		JAXBElement<TtmActions> ttmActions = null;

		// Validation schema
		JAXBContext jaxbContext;
		Unmarshaller unmarshaller = null;
		Schema schema = null;
		SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Validator validator = null;
		Source source = null;
		StreamSource xmlSource = null ;				

					
		inputFile = inputDirectory.listFiles(new TTMFileFilter())[0];
		
		 
		// Obtain resource and its reference to load included schemas
		source = new StreamSource(TTMIFHandler.class.getResourceAsStream("TtmInputCatalog.xsd"),
				TTMIFHandler.class.getResource("TtmInputCatalog.xsd").toString());
			
		try {						
			schema = factory.newSchema(source);
			validator = schema.newValidator();
			xmlSource = new StreamSource(inputFile);
			try {
				validator.validate(xmlSource);				
				validator.reset();
				try {
					jaxbContext = JAXBContext.newInstance("com.xpress.ttm.beans");
					unmarshaller = jaxbContext.createUnmarshaller();
					ttmActions = (JAXBElement<TtmActions>) unmarshaller.unmarshal(inputFile);
					
					if(ttmActions.getValue().getTtmActionList().size() == 0){
					// Reject the file
						ttmIFHandlerLogger.info("The file \""+ inputFile.getName() +"\" contains no actions.");
						if (!moveInputFile('I', 'R', inputFile.getName())) {
							ttmIFHandlerLogger.error("The file "+ inputFile.getName()+ " can not be unmarshalled");
						}						
					// accept the file
					}else {
						if (!moveInputFile('I', 'T', inputFile.getName())) {
							ttmIFHandlerLogger.error("The file "+ inputFile.getName()+ " can not be moved");
						}						
					}
					
				} catch (JAXBException e) {				
					if(e.getCause()!=null)
						ttmIFHandlerLogger.error("Failed to load: \"" + inputFile.getName() 
								+ "\" invalid input file \n" + e.getCause().getMessage());
					else 
						ttmIFHandlerLogger.error("Failed to load: \"" + inputFile.getName() 
								+ "\" invalid input file \n" ,e);
					
					// Reject file
					if (!moveInputFile('I', 'R', inputFile.getName())) {
						ttmIFHandlerLogger.error("The file " + inputFile.getName() + " can not be moved");
					} else {
						removeFile();					
					}
				}												
			} catch (SAXException e) {
				validator.reset();
				if(e.getCause()!=null)
					ttmIFHandlerLogger.error("Failed to parse: \"" + inputFile.getName() 
							+ "\" invalid input file \n" + e.getCause().getMessage());
				else 
					ttmIFHandlerLogger.error("Failed to parse: \"" + inputFile.getName() 
							+ "\" invalid input file \n" + e.getMessage());
				// Reject file
				if (!moveInputFile('I', 'R', inputFile.getName())) {
					ttmIFHandlerLogger.error("The file " + inputFile.getName() + " can not be moved");
				} else {
					removeFile();
				}
				
			} catch (IOException e) {
				validator.reset();
				ttmIFHandlerLogger.error("Failed to load: \"" + inputFile.getName()
					+ "\"  file can not be loaded \n" + e.getMessage());				
			}
		} catch (SAXException e) {
			if(e.getCause() != null)
				ttmIFHandlerLogger.error("Failed to load: \"" + "TtmInputCatalog.xsd" 
						+ "\" Invalid Schema file \n" + e.getCause().getMessage());
			else 
				ttmIFHandlerLogger.error("Failed to load: \"" + "TtmInputCatalog.xsd" 
						+ "\" Invalid Schema file \n" + e.getMessage());						
		} catch (NumberFormatException exception){
			if(exception.getCause() != null)
				ttmIFHandlerLogger .error("Failed to parse: \"" + exception.getCause().getMessage());
			else
				ttmIFHandlerLogger .error("Failed to parse: \"" , exception);
		
			// Reject file
			if (!moveInputFile('I', 'R', inputFile.getName())) {
				ttmIFHandlerLogger.error("The file " + inputFile.getName() + " can not be moved");
			}else {
				removeFile();			
			}
		}					
	
		return ttmActions;
	}

	/*
	 * Check for the directory structure of the TTM tool this structure is 
	 * Configured via the ttm.properties file and passed to this method 
	 * by the TTMController
	 */
	public boolean initDirectoryCheckSequence(String rootPath, String workDir,
			String inputDir, String rejectedDir, String processedDir,
			String tempDir) {
		rootDirectory = new File(rootPath);

		if (rootDirectory.exists()) {
			
			initDirectoryStructure(rootPath , workDir , inputDir , processedDir , rejectedDir , tempDir);
			
			if (!workDirectory.exists()) {
				ttmIFHandlerLogger.fatal("\"WORK\" directory not found: " 
						+ workDirectory);
			} else if (!inputDirectory.exists()) {
				ttmIFHandlerLogger.fatal("\"INPUT\" directory not found: ");
			} else if (!tempDirectory.exists()) {
				ttmIFHandlerLogger.fatal("\"TEMP\" directory not found "
						+ tempDirectory.getPath());
			} else if (!processedDirectory.exists()) {
				ttmIFHandlerLogger.fatal("\"PROCESSED\" directory not found ");
			} else if (!rejectedDirectory.exists()) {
				ttmIFHandlerLogger.fatal("\"REJECTED\" directory not found ");
			} else {
				return true;
			}
		} else {
			ttmIFHandlerLogger.fatal("Invalid Root directory: "
					+ rootPath + " is not found");
		}
		return false;
	}

	private void initDirectoryStructure(String rootPath, String workDir, String inputDir, String processedDir, String rejectedDir, String tempDir) {

		workDirectory = new File(rootPath + "/" + workDir);
		inputDirectory = new File(workDirectory.getPath() + "/" + inputDir);
		processedDirectory = new File(workDirectory.getPath() + "/"+ processedDir);
		rejectedDirectory = new File(workDirectory.getPath() + "/"+ rejectedDir);
		tempDirectory = new File(workDirectory.getPath() + "/" + tempDir);			
	}

	/*
	 * This function moves according to its status (TEMP , PROCESSED , REJECTED)
	 */
	public boolean moveInputFile(char source, char target, String input) {

		File inputFile;
		boolean moved = false;

		if (source == 'I') {
			inputFile = new File(inputDirectory.getPath() + "/" + input);
			if (target == 'T') {
				File newFile = new File(tempDirectory.getPath() + "/" + input);
				// check if file exists
				if (newFile.exists()) {
					newFile.delete();
				}
				moved = inputFile.renameTo(newFile);
			} else if (target == 'R') {
				if (inputFile.exists()) {
					File newFile = new File(rejectedDirectory.getPath() + "/"
							+ input);
					// check if file exists
					if (newFile.exists()) {
						newFile.delete();
					}
					moved = inputFile.renameTo(newFile);
				}
			} else {
				ttmIFHandlerLogger.info("move file wrong call");
			}
		} else if (source == 'T') {
			inputFile = new File(tempDirectory.getPath() + "/" + input);
			if (target == 'P') {
				File newFile = new File(processedDirectory.getPath() + "/"
						+ input);
				// check if file exists
				if (newFile.exists()) {
					newFile.delete();
				}
				moved = inputFile.renameTo(newFile);
			} else if (target == 'R') {
				File newFile = new File(rejectedDirectory.getPath() + "/"
						+ input);
				// check if file exists
				if (newFile.exists()) {
					newFile.delete();
				}
				moved = inputFile.renameTo(newFile);
			} else {
				ttmIFHandlerLogger.info("move file wrong call");
			}
		} else {
			ttmIFHandlerLogger.info("move file wrong call");
		}

		return moved;
	}

	/*
	 * Returns the file name which contained the passed action
	 */
	public String getFileName(String action) {
		return inputFile.getName();
	}
	
	/*
	 *  Remove The file from the currently processing
	 */
	public void  removeFile() {
		TTMFileFilter.remove(inputFile.getName());
	}
	
	public <T> void generateInputFile(JAXBElement<T> element) {
		JAXBContext jaxbContext;				
		Marshaller marshaller ;		
		
		// element.getName().getLocalPart().toLowerCase() 
		File outputFile = new File(inputDirectory + "/" + "TtmActions" + ".xml");
				
		try {
			jaxbContext = JAXBContext.newInstance("com.xpress.ttm.beans");							
			
			marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);
			marshaller.setProperty(Marshaller.JAXB_ENCODING,"UTF-8");											
			
			ttmIFHandlerLogger.info("Generating input file for action: " + element.getName().getLocalPart());			
			marshaller.marshal(element , outputFile);
			
			if(outputFile.exists()){
				ttmIFHandlerLogger.info("input file: \""+outputFile.getAbsolutePath()
						+"\" for action: "+ element.getName().getLocalPart()+ " created successfully");
			} else {
				ttmIFHandlerLogger.error("Failed to create input file: \""+outputFile.getAbsolutePath()
						+"\" for action: "+ element.getName().getLocalPart());
			}
			TTMFileFilter.setMarshalling(true);
		} catch (JAXBException e) {
			ttmIFHandlerLogger.error("Failed to generate file \""+ outputFile.getAbsolutePath()+"\": " , e);			
		}			
	}
}