package data.common.db;

import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class MongoConnector {

	private static MongoClient mongoClient;
	private static MongoDatabase database;
	
	public static void openConnection() {
		if(mongoClient != null) {
			return;
		}
		
		MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
		optionsBuilder.connectTimeout(50 * 1000);
		MongoClientURI connectionString = new MongoClientURI(System.getenv("MONGO_DATABASE_URL"), optionsBuilder);
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase database = mongoClient.getDatabase("si1718-jpg-publications");
		MongoConnector.mongoClient = mongoClient;
		MongoConnector.database = database;
	}
	
	public static boolean repairDatabase() {
		openConnection();
		Document result = database.runCommand(new BasicDBObject("repairDatabase", 1));
		if(result != null) {
			Double code = (Double) result.get("ok");
			if(code != null && code.equals(1.0D)) {
				return true;
			}
		}
		return false;
	}

	public static void closeConnection() {
		mongoClient.close();
	}

	public static MongoDatabase getDatabase() {
		return database;
	}
}
