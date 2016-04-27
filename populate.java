import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.sql.*;
import static java.lang.Math.toIntExact;

public class populate {
	
	public populate (String file1, String file2, String file3, String file4){
		//route1=file4;
		//route2=file1;
		//route3=file2;
		//route4=file3;
	}
	
	public static BufferedReader file1=null;
	public static BufferedReader file2=null;
	public static BufferedReader file3=null;
	public static BufferedReader file4=null;
	
	public static Connection con;
	public static JSONParser jsonParser;
	public static ArrayList<String> category;
	public static ArrayList<String> subcategory;
	
	public static void main(String[] args) {
		File route1 =new File(args[3]);
		File route2 =new File(args[0]);
		File route3 =new File(args[1]);
		File route4 =new File(args[2]);
		
		Connection con=null;
		String line=null;
		try {
			con=openConnection();
			
			DeleteEverything(con);
			
			//PARSES AND POPULATES WITH YELP_USER.JSON
			file1 =  new BufferedReader(new FileReader(route1));
			line=file1.readLine();
			System.out.println("Starting to parse and populate yelp_user.json");
			while(line!=null){
				YelpUser(line,con);
				YelpUserFriends(line, con);
				YelpUserVotes(line, con);
				YelpUserElite(line, con);
				line=file1.readLine();
			}
			System.out.println("Done parsing and populating user file!");
			
			//PARSES AND POPULATES WITH YELP_BUSINESS.JSON
			file2 =  new BufferedReader(new FileReader(route2));
			line=file2.readLine();
			System.out.println("Starting to parse and populate yelp_business.json");
			while(line!=null){
				Business(line, con);
				BusinessNeighborhood(line, con);
				BusinessCategories(line, con);
				line=file2.readLine();
			}
			System.out.println("Done parsing and populating business file!");
			
			//PARSES AND POPULATES YELP_REVIEW.JSON
			file3 =  new BufferedReader(new FileReader(route3));
			line=file3.readLine();
			System.out.println("Starting to parse and populate yelp_review.json");
			while(line!=null){
				Reviews(line, con);
				line=file3.readLine();
			}
			System.out.println("Done parsing and populating reviews file!");
			
			//PARSES AND POPULATES YELP_CHECKIN.JSON
			file4 =  new BufferedReader(new FileReader(route4));
			line=file4.readLine();
			System.out.println("Starting to parse and populate yelp_checkin.json");
			while(line!=null){
				CheckIn(line, con);
				line=file4.readLine();
			}
			System.out.println("Done parsing and populating checkin file!");
			
			System.out.println("DONE WITH EVERYTHING!");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			System.err.println("Errors when parsing the JSON file: " + e.getMessage()); 
	    } catch (IOException e) { 
	        System.err.println("IOException: " + e.getMessage()); 
	    } catch (SQLException e) { 
	        //line=file.readLine();
	    	System.err.println("Errors occurs when communicating with the database server: " + e.getMessage()); 
	    } catch (ClassNotFoundException e) { 
	        System.err.println("Cannot find the database driver"); 
	    } finally{ 
            closeConnection(con); 
        }
	}
	
	private static void DeleteEverything(Connection con) throws SQLException, ParseException{
		  Statement stmt = con.createStatement(); 
          System.out.println("Deleting previous tuples"); 
          System.out.println("This will take some time! Hang tight!");
          stmt.executeUpdate("DELETE FROM CHECK_IN");
          System.out.println("Done deleting from CHECK_IN! 8 tables to go");
          
          stmt.executeUpdate("DELETE FROM REVIEW");
          System.out.println("Done deleting from REVIEW! 7 tables to go");
          
          stmt.executeUpdate("DELETE FROM NEIGHBORHOOD");
          System.out.println("Done deleting from NEIGHBORHOOD! 6 tables to go");
          
          stmt.executeUpdate("DELETE FROM BUSINESS_CATEGORIES");
          System.out.println("Done deleting from BUSINESS_CATEGORIES! 5 tables to go");
          
          stmt.executeUpdate("DELETE FROM BUSINESS");
          System.out.println("Done deleting from BUSINESS! 4 tables to go");
      
          stmt.executeUpdate("DELETE FROM YELP_USER_ELITE");
          System.out.println("Done deleting from YELP_USER_ELITE! 3 tables to go");
          
          stmt.executeUpdate("DELETE FROM YELP_USER_FRIENDS");
          System.out.println("Done deleting from YELP_USER_FRIENDS! 2 tables to go");
          
          stmt.executeUpdate("DELETE FROM USER_VOTES");
          System.out.println("Done deleting from USER_VOTES! Just 1 table to go");
          
          stmt.executeUpdate("DELETE FROM YELPUSER");
          System.out.println("Done deleting from all tables!");
          stmt.close();
	}
	
	private static void YelpUser(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
		String yelping_since = (String) jsonObject.get("yelping_since");
        Long review_count = (long) jsonObject.get("review_count");
        String name = (String) jsonObject.get("name");
        String user_id = (String) jsonObject.get("user_id");
        Long fans = (long) jsonObject.get("fans");
        Double avgstars = (Double) jsonObject.get("average_stars");
        
        PreparedStatement stmt = con.prepareStatement("INSERT INTO YELPUSER VALUES(?,?,?,?,?,?)"); 
        
        stmt.setString(1, yelping_since);
        stmt.setLong(2,review_count);
        stmt.setString(3, name);
        stmt.setString(4, user_id);
        stmt.setLong(5, fans);
        stmt.setDouble(6, avgstars);
        stmt.executeUpdate();
        stmt.close();
	}	
	
	private  static void YelpUserFriends(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
        String user_id = (String) jsonObject.get("user_id");
        JSONArray friendList = (JSONArray) jsonObject.get("friends");
       
        PreparedStatement stmt = con.prepareStatement("INSERT INTO YELP_USER_FRIENDS VALUES(?,?)"); 
        stmt.setString(1, user_id);
        Iterator<String> iterator = friendList.iterator();
        String friend=null;
        int friendcount=0;
        while (iterator.hasNext()) {
        	friend=iterator.next();
        	friendcount++;
        }
        stmt.setLong(2, friendcount);
        stmt.executeUpdate();
        stmt.close();
	}
	
	private  static void YelpUserVotes(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		String user_id = (String) jsonObject.get("user_id");
        
		JSONObject votes=(JSONObject)jsonObject.get("votes");
        
        Long funny=(Long)votes.get("funny");
        Long useful=(Long)votes.get("useful");
        Long cool=(Long)votes.get("cool");
        
        PreparedStatement stmt = con.prepareStatement("INSERT INTO USER_VOTES VALUES(?,?,?,?)"); 
        stmt.setString(1, user_id);
        stmt.setLong(2, funny);
        stmt.setLong(3, useful);
        stmt.setLong(4, cool);
        stmt.executeUpdate();
        stmt.close();
	}
	
	private  static void YelpUserElite(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
        String user_id = (String) jsonObject.get("user_id");
        JSONArray elite = (JSONArray) jsonObject.get("elite");
       
        PreparedStatement stmt = con.prepareStatement("INSERT INTO YELP_USER_ELITE VALUES(?,?)"); 
        stmt.setString(1, user_id);
        Iterator<Long> iterator = elite.iterator();
        Long year = null;
        while (iterator.hasNext()) {
        	year = iterator.next();
        	stmt.setLong(2, year);
        	stmt.executeUpdate();
        }
        stmt.close();
	}
	
	private  static void Business(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
		String business_id = (String) jsonObject.get("business_id");
        String full_address = (String) jsonObject.get("full_address");
        String city = (String) jsonObject.get("city");
        Long review_count = (long) jsonObject.get("review_count");
        String name = (String) jsonObject.get("name");
        Double longitude = (Double) jsonObject.get("longitude");
        String state = (String) jsonObject.get("state");
        Double stars=(Double) jsonObject.get("stars");
        Double latitude = (Double) jsonObject.get("latitude");
        
        PreparedStatement stmt = con.prepareStatement("INSERT INTO BUSINESS VALUES(?,?,?,?,?,?,?,?,?)"); 
        
        stmt.setString(1, business_id);
        stmt.setString(2, full_address);
        //stmt.setString(3, open);
        stmt.setString(3, city);
        stmt.setLong(4,review_count);
        stmt.setString(5, name);
        stmt.setDouble(6, longitude);
        stmt.setString(7, state);
        stmt.setDouble(8, stars);
        stmt.setDouble(9, latitude);
        stmt.executeUpdate();
        stmt.close();
	}
	
	private  static void BusinessNeighborhood(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
        String business_id = (String) jsonObject.get("business_id");
        JSONArray neighborhoods = (JSONArray) jsonObject.get("neighborhoods");
       
        PreparedStatement stmt = con.prepareStatement("INSERT INTO NEIGHBORHOOD VALUES(?,?)"); 
        stmt.setString(1, business_id);
        Iterator<String> iterator = neighborhoods.iterator();
        String neighborhood=null;
        while (iterator.hasNext()) {
        	neighborhood=iterator.next();
        	stmt.setString(2, neighborhood);
        	stmt.executeUpdate();	
        }
        stmt.close();
	}
	
	private  static void BusinessCategories(String line, Connection con) throws SQLException, ParseException{
		//String l = line;
		jsonParser = new JSONParser();
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
        String business_id = (String) jsonObject.get("business_id");
        JSONArray categories = (JSONArray) jsonObject.get("categories");
       
        PreparedStatement stmt = con.prepareStatement("INSERT INTO BUSINESS_CATEGORIES VALUES(?,?,?)"); 
        stmt.setString(1, business_id);
        Iterator<String> iterator = categories.iterator();
        category=new ArrayList<>();
        subcategory=new ArrayList<>();
        String check=null;
        while (iterator.hasNext()) {
        	check=iterator.next();
        	if(check.equals("Active Life") || check.equals("Arts & Entertainment") || check.equals("Automotive")||check.equals("Car Rental")||check.equals("Cafes")||check.equals("Beauty & Spas")||check.equals("Convenience Stores")||check.equals("Dentists")||check.equals("Doctors")||check.equals("Drugstores")||check.equals("Department Stores")||check.equals("Education")||check.equals("Event Planning & Services")||check.equals("Flowers & Gifts")||check.equals("Food")||check.equals("Health & Medical")||check.equals("Home Services")||check.equals("Home & Garden")||check.equals("Hospitals")||check.equals("Hotels & Travel")||check.equals("Hardware Stores")||check.equals("Grocery")||check.equals("Medical Centers")||check.equals("Nurseries & Gardening")||check.equals("Nightlife")||check.equals("Restaurants")||check.equals("Shopping")||check.equals("Transportation")){
        		category.add(check);
        	}
        	else{
        		subcategory.add(check);
        	}
        }
        
        for (String c: category){
        	stmt.setString(2,  c);
        	if(subcategory.isEmpty()){
        		String trash="bleh";
        		stmt.setString(3,  trash);
        		stmt.executeUpdate();
        	}
        	else{
        		for (String s: subcategory){
        			stmt.setString(3, s);
        			stmt.executeUpdate();
        		}
        		
        	}
        }
        stmt.close();
	}
	
	private  static void Reviews(String line, Connection con) throws SQLException, ParseException{
		jsonParser = new JSONParser();
		
		JSONObject jsonObject =(JSONObject)jsonParser.parse(line);
		
		JSONObject votes=(JSONObject)jsonObject.get("votes");
		long useful=(long)votes.get("useful");
		long funny=(long)votes.get("funny");
		long cool=(long)votes.get("cool");
		long total=useful+funny+cool;
		String user_id=(String)jsonObject.get("user_id");
		String review_id=(String)jsonObject.get("review_id");
		long stars=(long)jsonObject.get("stars");
		String date=(String)jsonObject.get("date");
		String business_id=(String)jsonObject.get("business_id");
		String text=(String)jsonObject.get("text");
		
        PreparedStatement stmt = con.prepareStatement("INSERT INTO REVIEW VALUES(?,?,?,?,?,?,?)"); 
        stmt.setLong(1,total);
        stmt.setString(2, user_id);
        stmt.setString(3, review_id);
        stmt.setLong(4,stars);
        stmt.setString(5, date);
        stmt.setString(6, business_id);
        stmt.setString(7, text);
        stmt.executeUpdate();
        stmt.close(); 
	}	
	
	private  static void CheckIn(String line, Connection con) throws SQLException, ParseException{
		jsonParser   = new JSONParser();
		JSONObject jsonObject   = (JSONObject) jsonParser.parse(line);
		JSONObject structure = (JSONObject) jsonObject.get("checkin_info");
		Set<String> set = structure.keySet();
		ArrayList<String> list  = new ArrayList<String>(set);
		PreparedStatement stmt = con.prepareStatement("INSERT INTO CHECK_IN VALUES(?,?,?,?)");
		String business_id = (String) jsonObject.get("business_id");
		stmt.setString(1, business_id);
		
		for(int i = 0 ; i < list.size() ; i++){
				String listItem = list.get(i);
				String[] tokens = listItem.split("-"); 
				long from_hour = Long.valueOf(tokens[0]);
				int from_hour2=toIntExact(from_hour);
				
				long day=Long.valueOf(tokens[1]);
				int day2=toIntExact(day);
				
				long checkin_count = (long)structure.get(listItem);
				int checkin_count2=toIntExact(checkin_count);
				
				stmt.setInt(2, day2);
				stmt.setInt(3, from_hour2);
				stmt.setInt(4, checkin_count2);
				stmt.executeUpdate();
				
		}
		stmt.close();
	}
	
	private static Connection openConnection() throws SQLException, ClassNotFoundException { 
	       DriverManager.registerDriver(new oracle.jdbc.OracleDriver()); 
 
	       String host = "localhost"; 
	       String port = "1521"; 
	       String dbName = "oracledb"; 
	       String userName = "hr"; 
	       String password = "blank"; 

	      // Construct the JDBC URL 
	      String dbURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName; 
	      return DriverManager.getConnection(dbURL, userName, password); 
	  } 
	 
	public static void closeConnection(Connection con) { 
	      try { 
	          con.close(); 
	      } catch (SQLException e) { 
	          System.err.println("Cannot close connection: " + e.getMessage()); 
	      } 
	  } 
}