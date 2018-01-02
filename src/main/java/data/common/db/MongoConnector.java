package data.common.db;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnector {

	private static MongoClient mongoClient;
	private static MongoDatabase database;
	
	private static MongoCollection<Document> articlesCollection;
	
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
	
	public static void openArticlesConnection() {
		if(MongoConnector.articlesCollection != null) {
			return;
		}
		MongoConnector.openConnection();
		MongoCollection<Document> collection = MongoConnector.getDatabase().getCollection("articles");
		MongoConnector.articlesCollection = collection;
	}
	
	public static Iterable<Document> getAllArticles(){
		openArticlesConnection();
		FindIterable<Document> result = articlesCollection.find();
		return result;
	}
	
	public static Iterable<Document> getArticles(Long from, Long to){
		openArticlesConnection();
		Long limit = to - from;
		FindIterable<Document> result = articlesCollection.find().limit(limit.intValue()).skip(from.intValue());
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public static Set<String> getArticlesKeywords(){
		openArticlesConnection();
		Set<String> keywords = new HashSet<String>();
		Iterable<Document> articles = articlesCollection.find();
		for (Document e:articles) {
			if (e.containsKey("keywords")) {
				Object rawKeys = e.get("keywords");
				if(rawKeys != null && rawKeys instanceof List) {
					for (String s : (List<String>) rawKeys) {
						if(s != null) {
							keywords.add(s.trim().toLowerCase());
						}
					}
				}
			}
		}
		return keywords;
	}
}
