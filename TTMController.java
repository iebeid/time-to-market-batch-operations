package com.xpress.ttm.control;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

import javax.xml.bind.JAXBElement;

import org.apache.log4j.Logger;

import com.xpress.ttm.beans.Action;
import com.xpress.ttm.beans.ActionMapping;
import com.xpress.ttm.beans.ModChargeSchema;
import com.xpress.ttm.beans.ModUsagePackage;
import com.xpress.ttm.beans.ObjectFactory;
import com.xpress.ttm.beans.TtmAction;
import com.xpress.ttm.beans.TtmActions;
import com.xpress.ttm.handlers.mp.CopyUsagePackageHandler;
import com.xpress.ttm.handlers.mp.RefreshRatingPackageHandler;
import com.xpress.ttm.utils.ActionHandler;



public class TTMController {

	/*
	 * Handles all files ; mapping and input files
	 */
	private TTMIFHandler inputFileHandler = new TTMIFHandler();
	/*
	 * 
	 */
	private ObjectFactory objectFactory = new ObjectFactory();
	/*
	 * Logger
	 */
	static final Logger controlLogger = Logger.getLogger(TTMController.class);
	/*
	 * TTM properties
	 */
	Properties properties = new Properties();
	
	private static Comparator<Action> SORT_MAPPING_COMPARATOR = new Comparator<Action>(){

		public int compare(Action o1, Action o2) {
			int returnValue = o1.getActionProcedure().compareToIgnoreCase(
					o2.getActionProcedure());
			if (returnValue != 0)
				return returnValue;
			return (o1.getActionHandler().getClass().getCanonicalName()
					.compareToIgnoreCase(o2.getActionHandler().getClass().getCanonicalName()));
		}
		
	};

	public static void main(String[] args) {
		TTMController controller = new TTMController();

		if (args.length < 1) {
			System.err.println("Missing argument: TTM root directory is missing");
			System.exit(1);
		} else if (args.length > 1){
			System.err.println("Too many arguments");
			System.exit(1);
		} else {		
			controller.start(args[0]);
			System.exit(0);
		}
	}

	/*
	 * For each action in the mapping file : - Get the input parameters
	 * (Action)-->(TtmAction) - Get ActionHadler - Assign TTmAction to
	 * Corresponding ActionHandler --> add to "ttmActions" Map
	 */
	private void start(String rootPath) {

		ActionHandler ttmActionHandler;
		JAXBElement<TtmActions> ttmActions = null;
		ActionMapping actionMapping;
		Action action;
		String inputFileName = null;

		String driverName = "oracle.jdbc.driver.OracleDriver";
		Connection connection = null;

		// Read tool Configuration
		controlLogger.info("Reading tool configuration: \"ttm.properties\"");
		if (!this.configure()) {
			System.exit(1);
		}

		// Create database connection (Shared among handlers)
		controlLogger.info("Configuring data base connection: "+ properties.getProperty("connection.url"));
		
		try {
			Class.forName(driverName);
			connection = DriverManager.getConnection(properties
					.getProperty("connection.url"), properties
					.getProperty("connection.username"), properties
					.getProperty("connection.password"));
			connection.setAutoCommit(false);
			controlLogger.info("Connection Auto Commit = " + connection.getAutoCommit());
		} catch (ClassNotFoundException e) {
			controlLogger.fatal("Failed to load driver: " + driverName, e);
			System.exit(1);
		} catch (SQLException e) {
			controlLogger.fatal("Failed to connect to database ", e);
			System.exit(1);
		}
		
		controlLogger.info("Checking directory structure");
		if (inputFileHandler.initDirectoryCheckSequence(rootPath, properties
				.getProperty("ttm.work.directory"), properties
				.getProperty("ttm.work.input.directory"), properties
				.getProperty("ttm.work.rejected.directory"), properties
				.getProperty("ttm.work.processed.directory"), properties
				.getProperty("ttm.work.temp.directory"))) {							
			
			actionMapping = inputFileHandler.configureActionMapping();
														
	
			if (actionMapping != null) {
				// Indicates that there are no more input files to read 
				boolean finished = false;		
				
				// Indicates that a roll back took place to end this transaction 
				boolean rollback = false ;
				
				Collections.sort(actionMapping.getAction() , SORT_MAPPING_COMPARATOR); 										
				
				while (!finished){
					
					controlLogger.info("Loading input files");
					try {
						rollback = false ;
						ttmActions = inputFileHandler.confgiureActions();					
					} catch (ArrayIndexOutOfBoundsException exception){
						ttmActions = null ;
						finished = true ;
						controlLogger.info("No input files were loaded");
					}

					if(ttmActions != null){
						if (ttmActions.getValue().getTtmActionList().size() != 0) {																															
							for (TtmAction ttmAction : ttmActions.getValue().getTtmActionList()) {																	
								inputFileName = inputFileHandler.getFileName(ttmAction.getClass().getSimpleName());						

								controlLogger.info("Proccessing action: "
										+ ttmAction.getClass().getSimpleName()+ " Input file: " + inputFileName);
								controlLogger.info("Reading Catalog entry for action: " 
										+ ttmAction.getClass().getSimpleName());
								
								action = getMappingAction(actionMapping , ttmAction);
																			
								if (action != null) {
									controlLogger.info("Loading handler: "+ action.getActionHandler() 
											+ " for action "+ action.getActionProcedure());
									ttmActionHandler = getActionHandler(action);

									if (ttmActionHandler != null) {
										// Set Handler "dateFromat" property
										if (properties.containsKey("ttm.date.format") && 
												properties.getProperty("ttm.date.format") != null) {

											if (!isDatedHandler(ttmActionHandler)) {
												controlLogger.info(action.getActionHandler()+ ": defaulting to MM/dd/yyyy");
											} else {
												controlLogger.info(action.getActionHandler() + ": date format is set to: "
														+ properties.getProperty("ttm.date.format"));

											}
										} else {
											controlLogger.info(action.getActionHandler()
													+ ": Date is not found in \"ttm.properties\"or invalid defaulting to MM/dd/yyyy");
										}
										
										// Set Handler "userName" property if exists						
										isUserNameHandler(ttmActionHandler);
										
										
										if (ttmActionHandler.handle(ttmAction , action.getActionProcedure(), connection)) {											
											if(action.getActionProcedure().equals("CopyUsagePackage")){
												CopyUsagePackageHandler copyUsagePackageHandler = (CopyUsagePackageHandler) ttmActionHandler ;
												ModUsagePackage modUsagePackageAction = copyUsagePackageHandler.getModUsagePackageElement(connection) ;
												
												if(modUsagePackageAction != null){
													
													TtmActions outTtmActions = objectFactory.createTtmActions();												
													outTtmActions.getTtmActionList().add(modUsagePackageAction);																								
													JAXBElement<TtmActions> element = objectFactory.createTtmActions(outTtmActions);																						
													inputFileHandler.generateInputFile(element);
												}									
											}else if (action.getActionProcedure().equals("RefreshRatingPackage")){
												RefreshRatingPackageHandler refreshRatingPackageHandler = (RefreshRatingPackageHandler) ttmActionHandler;
												ModChargeSchema modChargeSchema = refreshRatingPackageHandler.getModChargeSchemaElement();
												
												if(modChargeSchema != null){
													TtmActions outTtmActions = objectFactory.createTtmActions();												
													outTtmActions.getTtmActionList().add(modChargeSchema);
													JAXBElement<TtmActions> element = objectFactory.createTtmActions(outTtmActions);
													inputFileHandler.generateInputFile(element);
												}
											}
										} else {
											try {
												finished = true ;
												rollback = true;
												controlLogger.info("Rollback");
												connection.rollback();												
											} catch (SQLException e) {
												controlLogger.error("SQL Error: Rollback failed ",e);
											}
											if (!inputFileHandler.moveInputFile('T', 'R',
													inputFileName)) {
												controlLogger.error("The file \""+ inputFileName+ "\" can not be moved");
											} else {
												inputFileHandler.removeFile();
											}
											break ;										
										}
									} else {
										controlLogger.fatal("Loading Action Handler: "+ action.getActionHandler() + " failed");
										if (!inputFileHandler.moveInputFile('T', 'R',
												inputFileName)) {
											controlLogger.error("The file \""+ inputFileName+ "\" can not be moved");
										} else {
											inputFileHandler.removeFile();
										}
										rollback = true ;
									}
								} else {
									controlLogger.error("Loading Action: "+ ttmAction.getClass().getSimpleName()
											+ " failed , the specified action isn't found in the Catalog file \"action-mapping.xml\"");
									rollback = true ;
								}								
							}
							if(!rollback){
								try {
									controlLogger.info("Commit");
									connection.commit();											
								} catch (SQLException e) {
									controlLogger.error("SQL Error: Commit failed ",e);
								}						
								if (!inputFileHandler.moveInputFile('T', 'P',inputFileName)) {
									controlLogger.error("The file \""+ inputFileName+ "\" can not be moved");
								} else {
									inputFileHandler.removeFile();
								}
							}
						}
					} else {
						finished = true ;
					}
				}
			} else {
				controlLogger.fatal(" \"action-mapping.xml\" was not loaded");
			}
		} 
		
		try {
			controlLogger.info("Closing database connection");
			connection.close();
		} catch (SQLException e) {
			controlLogger.error("Failed to close database connection", e);			
		}
		
		controlLogger.info("Closing");		
	}
	

	/*
	 * Binary search "action-mapping.xml" for the specified action 
	 */
	private Action getMappingAction(ActionMapping actionMapping,
			TtmAction action ) {
		int index = Collections.binarySearch(actionMapping.getAction(), action.getClass().getSimpleName());	
		if(index < 0){
			return null ;
		}
		return actionMapping.getAction().get(index);
	}
	/*
	 * Check if the current handler utilizes date to set its "dateFormat" property 
	 */
	private boolean isDatedHandler(ActionHandler handler) {
		try {
			Method setDateFormat = handler.getClass().getMethod(
					"setDateFormat", String.class);
			if (setDateFormat != null) {
				try {
					setDateFormat.invoke(handler, properties
							.getProperty("ttm.date.format"));
					return true;
				} catch (IllegalArgumentException e) {
					controlLogger.error(e.getMessage());
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					controlLogger.error(e.getMessage());
					// e.printStackTrace();
				} catch (InvocationTargetException e) {
					controlLogger.error(e.getMessage());
					// e.printStackTrace();
				}
			}
		} catch (SecurityException e) {
			//controlLogger.debug(e.getMessage());
		} catch (NoSuchMethodException e) {
			//controlLogger.debug(handler.getClass().getName() + "is not ");
		}
		return false;
	}

	/*
	 * Check if the current handler utilizes user-name to set its "userName" property
	 */
	private boolean isUserNameHandler(ActionHandler handler) {
		try {
			Method setUserName = handler.getClass().getMethod(
					"setUserName", String.class);			
			try {
				setUserName.invoke(handler, properties
						.getProperty("connection.username"));
				return true;
			} catch (IllegalArgumentException e) {
				controlLogger.debug(e);				
			} catch (IllegalAccessException e) {
				controlLogger.debug(e);				
			} catch (InvocationTargetException e) {
				controlLogger.debug(e);
			}			
		} catch (SecurityException e) {
			controlLogger.debug(e);
		} catch (NoSuchMethodException e) {
			//controlLogger.debug(e);
		}
		return false;
	}

	/*
	 * Read the properties file
	 */
	@SuppressWarnings("unchecked")
	private ActionHandler getActionHandler(Action action) {
		Class<ActionHandler> actionHandlerClass;
		ActionHandler ttmActionHandler = null;

		try {
			actionHandlerClass = (Class<ActionHandler>) TTMController.class
					.getClassLoader().loadClass(action.getActionHandler());
			// -----------------------------------------------------------
			ttmActionHandler = actionHandlerClass.getConstructor()
					.newInstance();
		} catch (ClassNotFoundException e) {
			controlLogger.fatal("Class " + action.getActionHandler()
					+ " is not found ");
		} catch (SecurityException e) {
			controlLogger.fatal(e);
		} catch (NoSuchMethodException e) {
			controlLogger.fatal(e);
		} catch (IllegalArgumentException e) {
			controlLogger.fatal(e);
		} catch (InstantiationException e) {
			controlLogger.fatal(e);
		} catch (IllegalAccessException e) {
			controlLogger.fatal(e);
		} catch (InvocationTargetException e) {
			controlLogger.fatal(e);
		}
		return ttmActionHandler;
	}
	
	
	/*
	 * Find the ActionHandler type and instantiate an object of it
	 */
	private boolean configure() {
		try {
			properties.load(new FileInputStream("ttm.properties"));
			return true;
		} catch (FileNotFoundException e) {
			controlLogger.fatal("\"ttm.properties\" does not exist");
			return false;
		} catch (IOException e) {
			controlLogger.fatal("\"ttm.properties\" can't be loaded", e);
			return false;
		}	
	}
}
