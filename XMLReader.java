package ingestion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import dataStructures.RatedKeyWord;
import dataStructures.Website;

/**
 * XMLReader to validate XML´s and read them with SAX Parser
 * Writes Fields from XML according to Config into Website Object Members (Project Specific Requirement!)
 * 
 * Can validate an XML File using XSD or DTD, decides for itself which one is needed 
 *
 * See danielabram.de for sequence diagram and further informations
 * @author D. Abram
 * 
 */

public class XMLReader implements Runnable{
	
	private File xml;
	private IngestionSupervisor supervisor;
	private Website website;
	
	private boolean[] bHeadMetaTags;
	
	private final Logger logger = Logger.getLogger(this.getClass().getName());

	/**
	 * 
	 * @param supervisor the Supervisor of this XMLReader for Managing the Queues
	 * @param xml the File that is going to be read and validated
	 */
	public XMLReader(IngestionSupervisor supervisor, File xml){
		this.xml = xml;
		this.supervisor = supervisor;
		website = new Website(null);
		website.setXml(true);
		bHeadMetaTags = new boolean[supervisor.getCo_HeadMetaTags().size()];
		for(int i=0;i<bHeadMetaTags.length;i++){
			bHeadMetaTags[i] = false;		
		}
	}
	/**
	 * Checks if the XML is already in the Database
	 * If it is not, it calls {@link readXML()} and puts the Website Object into the Queue of the Supervisor
	 */
	@Override
	public void run(){
		website.setXml(true);
		website.setCorrectXML(true); //set to true, pretend everything is going correctly in the following steps, if something fails set this to false!
		website.setCurrentLocation(xml);
		if( supervisor.getDatabase().xmlAlredyInDatabase(website.getCurrentLocation()) == false ){
			readXML();
			if(website.isCorrectXML()){
				//Fill accordingly to your needs here
				//All is okay at this point!
				//The XML is now read and validated, you can discard it or anything else you wish
			}
			else{			
				//Here goes the Code for Logging or similar Behaviour
				//e.g. : logger.log(Level.INFO, "XML File not correctly read - Path:" + website.getCurrentLocation());
			}
			
		}
		else{
			//Here goes the Code for Logging or similar Behaviour
			//e.g. : logger.log(Level.INFO, "XML already in Database, path:" + website.getCurrentLocation());
		}
		
	}
	

	
	/**
	 * If the XML is valid {@link validate()} it starts parsing the XML 
	 */
	public void readXML(){
		if(validate()){
			
				SAXParser parser = null;
				try {
					parser = supervisor.getFactory().newSAXParser();
				} catch (ParserConfigurationException e) {
					website.setCorrectXML(false);
					//Here goes the Code for Logging or similar Behaviour
					//e.g.: logger.log(Level.SEVERE, "Could not Configure SAXParser", e);
				} catch (SAXException e) {
					website.setCorrectXML(false);
					//Here goes the Code for Logging or similar Behaviour
					//e.g.: logger.log(Level.WARNING, "SAXException before Parsing", e);
				}
			
				if(parser!=null){
					//Parse
					try {
						parser.parse(xml, Handler);
					} catch (SAXException e) {
						website.setCorrectXML(false);
						//Here goes the Code for Logging or similar Behaviour
						//e.g.: logger.log(Level.WARNING, "SAXException", e);
						
					} catch (IOException e) {
						website.setCorrectXML(false);
						//Here goes the Code for Logging or similar Behaviour
						//e.g.: logger.log(Level.FINE, "I/O Error with XML or DTD/XSD", e);
						e.printStackTrace();
					}
				}
			else{
				website.setCorrectXML(false);
				//Here goes the Code for Logging or similar Behaviour
				//e.g.: logger.log(Level.SEVERE, "Could not setup Parser");
				}
		}
		else{
			website.setCorrectXML(false);
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: logger.log(Level.INFO, "Non Valid XML");
			
		}
	}

	/**
	 * Validates an XML if it uses XSD as Shema
	 * XML with DTD Shemas will be validated on the Fly while parsing in the {@link DefaultHandler}
	 * @return true if it is Validated, <code>false</code> if not
	 */
	public boolean validate(){
		if(isXSD()){
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			try {
				Schema schema = factory.newSchema(); //sets XSD location on the Fly
				Validator validator = schema.newValidator();
				Source src = new StreamSource(xml); 
				validator.validate(src);
				return true;
			} catch (SAXException e) {
				website.setCorrectXML(false);
				//Here goes the Code for Logging or similar Behaviour
				//e.g.: logger.log(Level.INFO, "Nonvalid XML, or Schema Failure", e);
				return false;
			} catch (IOException e) {
				website.setCorrectXML(false);
				//Here goes the Code for Logging or similar Behaviour
				//e.g.: logger.log(Level.WARNING, "IO Exception while Validating",e);
				return false;
			}
			
		}
		else{
			//Validating on the Fly for XML with DTD Schema
			return true;
		}
	}
	
	/**
	 * Checks if an XML is using XSD as Validator
	 * First 3 Lines of the Document (XML) have to contain one of the following Strings:
	 * "xsi:noNamespaceSchemaLocation"
	 * "xsi:schemaLocation"
	 * to be registred as an XML that uses XSD as Shema
	 * @return <code>true</code> if it is xsd (finds the Strings), <code>false</code> if not
	 */
	public boolean isXSD(){
		BufferedReader br = null;
		String text = "";
		try {
			br = new BufferedReader(new FileReader(xml));
				for(int i=0;i<3;i++){
					String line = br.readLine();
					if (line!=null){

						text = text + line;
					}
				}
				br.close();
		} catch (FileNotFoundException e) {
			website.setCorrectXML(false);
			//Here goes the Code for Logging or similar Behaviour
			//e.g.:logger.log(Level.WARNING, "File not found", e);
		} catch (IOException e) {
			website.setCorrectXML(false);
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: logger.log(Level.WARNING, "Could not Read XML", e);
		}
		
		//search in line for schemalocation
		String searchA, searchB;
		searchA = "xsi:noNamespaceSchemaLocation";
		searchB = "xsi:schemaLocation"; 
		
		if(text.contains(searchA) || text.contains(searchB)){
			return true;
		}
		else{
			return false;
		}
		
		
	}
	
	/**
	 * Handles the XML Reading
	 */
	DefaultHandler Handler = new DefaultHandler(){
	
	boolean url = false;
	boolean title = false;
	boolean date = false;
	boolean rKWT = false;
		
	/**
	 * Handles the Start of an Element, sets a boolean if it finds a particular Element
	 * With that boolean Value {@link chacters(character[],String,String)} can decide what Action to take
	 * 
	 * @param uri - The Namespace URI, or the empty string if the element has no Namespace URI or if Namespace processing is not being performed.
     * @param localName - The local name (without prefix), or the empty string if Namespace processing is not being performed.
     * @param qName - The qualified name (with prefix), or the empty string if qualified names are not available.
     * @param attributes - The attributes attached to the element. If there are no attributes, it shall be an empty Attributes object. 
	 */
	@Override
	public void startElement(String uri, String localName,String qName, Attributes attributes){
		if(qName.equalsIgnoreCase(supervisor.getCo_URL())){
			url = true;
		}
		if(qName.equalsIgnoreCase(supervisor.getCo_CrawlerDate())){
			date=true;
		}
		if(qName.equalsIgnoreCase(supervisor.getCo_Titel())){
			title=true;
		}
		
		for(int i=0;i<supervisor.getCo_HeadMetaTags().size();i++){
			if(qName.equalsIgnoreCase(supervisor.getCo_HeadMetaTags().get(i))){
				bHeadMetaTags[i] = true;
			}
		}
		
		if(qName.equalsIgnoreCase(supervisor.getCo_ratedKeyWordTag())){
			rKWT = true;
		}
	}
	
	/**
	 * Reads the Characters of an Element and Decides what to do according to a boolean from {@link startElement(character[], String, String)}
	 * 
	 * @param ch - The characters.
     * @param start - The start position in the character array.
     * @param length - The number of characters to use from the character array
	 */
    
	@Override
	public void characters(char ch[], int start, int length){
		if(url){
			parseURL(ch, start, length);
			url = false;
		}
		if(title){
			website.setTitle(new String(ch, start, length));
			title = false;
		}
		if(date){
			parseDate(ch, start, length);
			date = false;
		}
		
		for(int i=0;i<bHeadMetaTags.length;i++){
			if(bHeadMetaTags[i]){
				website.getMetaInformations().put(supervisor.getCo_HeadMetaTags().get(i), new String(ch, start, length));
				bHeadMetaTags[i] = false;
			}
		}
		
		if(rKWT){
			//Project Specific Parsing
		}
	}
	
	
	/**
	 * 
	 * Reads a Date from a String and sets Website Member to it
	 * Uses SimpleDateFormat for Parsing the String
	 * 
	 * @param ch - The characters.
	 * @param start - The start position in the character array.
	 * @param length - The number of characters to use from the character array
	 */
	public void parseDate(char ch[], int start, int length){
			 String stringDate = new String(ch, start, length);
			 SimpleDateFormat sdf = new SimpleDateFormat(supervisor.getCo__Dateformat());
			 Date aDate;
			try {
				aDate = sdf.parse(stringDate);
				 website.setTimeStamp(aDate.getTime());
			} catch (ParseException e) {
				website.setCorrectXML(false);
				//Here goes the Code for Logging or similar Behaviour
				//e.g.: logger.log(Level.FINE, "Could not Parse Time in XML", e);
			}
			
	}
	
	/**
	 * Reads a URL from a String and sets Website Member to it
	 * 
	 * @param ch - The characters.
	 * @param start - The start position in the character array.
	 * @param length - The number of characters to use from the character array
	 */
	public void parseURL(char ch[], int start, int length){
		String sUrl = new String(ch, start, length);
		URL url;
		try {
			url = new URL(sUrl);
		} catch (MalformedURLException e) {
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: logger.log(Level.FINE, "Could not Parse Input String to URL", e);
			website.setCorrectXML(false);
			url = null;
		}
		website.setUrl(url);
	}
	
};
}
