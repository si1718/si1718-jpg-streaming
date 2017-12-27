package data.streaming.db;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;

import data.streaming.dto.ArticleRatingDTO;
import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;

public class MongoConnector {

	
	private static MongoCollection<Document> articlesCollection;
	private static MongoCollection<Document> tweetsCollection;
	private static MongoCollection<Document> recommendationsCollection;
	private static MongoCollection<Document> reportsCollection;
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
		Document result = database.runCommand(new BasicDBObject("repairDatabase", 1));
		if(result != null) {
			Double code = (Double) result.get("ok");
			if(code != null && code.equals(1.0D)) {
				return true;
			}
		}
		return false;
	}
	
	public static void openArticlesConnection() {
		if(MongoConnector.articlesCollection != null) {
			return;
		}
		openConnection();
		MongoCollection<Document> collection = database.getCollection("articles");
		MongoConnector.articlesCollection = collection;
	}
	
	public static void openTweetsConnection() {
		if(MongoConnector.tweetsCollection != null) {
			return;
		}
		openConnection();
		MongoCollection<Document> collection = database.getCollection("tweets");
		MongoConnector.tweetsCollection = collection;
	}
	
	public static void openRecommendationsConnection() {
		if(MongoConnector.recommendationsCollection != null) {
			return;
		}
		openConnection();
		MongoCollection<Document> collection = database.getCollection("recommendations");
		MongoConnector.recommendationsCollection = collection;
	}
	
	public static void openReportsConnection() {
		if(MongoConnector.reportsCollection != null) {
			return;
		}
		openConnection();
		MongoCollection<Document> collection = database.getCollection("reports");
		MongoConnector.reportsCollection = collection;
	}
	
	public static void closeConnection() {
		mongoClient.close();
	}
	
	public static boolean saveTweetOnDB(String tweet, boolean convert) {
		openTweetsConnection();
		String dto;
		if(convert) {
			dto = Utils.convertTweetToPublicationsFormat(tweet, false);
		} else {
			dto = tweet;
		}
		if(dto != null) {
			tweetsCollection.insertOne(Document.parse(dto));
		} else {
			System.err.println("Error creating tweet");
		}
		return true;
	}
	
	public static boolean saveReport(KeywordDTO keyword, boolean byMonth) throws JsonProcessingException {
		openReportsConnection();
		LocalDate date = keyword.getTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf;
		if(byMonth) {
			dayOf = date.getMonthValue();
		} else {
			dayOf = date.getDayOfYear();
		}
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(keyword);
		Document doc = Document.parse(json);
		if(byMonth) {
			doc.append("report_type", "tweetMonth");
		} else {
			doc.append("report_type", "tweet");
		}
		doc.append("report_year", year);
		doc.append("report_day", dayOf);
		KeywordDTO existing = getKeywordReport(keyword.getKeyword(), keyword.getTime(), byMonth);
		if(existing != null) {
			Double score = keyword.getStatistic() + existing.getStatistic();
			doc.put("statistic", score);
			System.out.println("Updating existing report on date: " + dayOf + "/" + year + " byMonth: " + byMonth + ". Old: " + existing.getStatistic() + " New: " + score);
			reportsCollection.replaceOne(getFilterForKeyword(keyword, byMonth), doc);
		} else {
			reportsCollection.insertOne(doc);
		}
		return true;
	}
	
	public static boolean saveRatings(List<ArticleRatingDTO> ratings) {
		ratings.forEach(x -> {
			try {
				saveRating(x);
			} catch (JsonProcessingException e) {
				System.err.println("Cannot save rating!");
				e.printStackTrace();
			}
		});
		return true;
	}
	
	public static boolean saveRating(ArticleRatingDTO rating) throws JsonProcessingException {
		openReportsConnection();
		ArticleRatingDTO existing = getArticleRating(rating.getArticleA(), rating.getArticleB());
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(rating);
		Document doc = Document.parse(json);
		doc.append("report_type", "articleRating");
		if(existing != null) {
			reportsCollection.replaceOne(getFilterForRating(rating), doc);
		} else {
			reportsCollection.insertOne(doc);
		}
		return true;
	}
	
	public static boolean saveRecommendation(ArticleRatingDTO recomm) throws JsonProcessingException {
		openRecommendationsConnection();
		ArticleRatingDTO existing = getRecommendation(recomm.getArticleA(), recomm.getArticleB());
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(recomm);
		Document doc = Document.parse(json);
		if(existing != null) {
			recommendationsCollection.replaceOne(getFilterForRecommendation(recomm), doc);
		} else {
			recommendationsCollection.insertOne(doc);
		}
		return true;
	}
	
	public static Long deleteNullTweets() {
		openTweetsConnection();
		Bson filterId = Filters.eq("text", null);
		DeleteResult result = tweetsCollection.deleteMany(filterId);
		return result.getDeletedCount();
	}
	
	public static boolean deleteListTweet(List<String> tweets) {
		for (String string : tweets) {
			deleteTweet(string);
		}
		return true;
	}
	
	public static boolean deleteTweet(String strId) {
		openTweetsConnection();
		Bson filterId = Filters.eq("text", strId);
		DeleteResult result = tweetsCollection.deleteOne(filterId);
		if(result.getDeletedCount() != 0L) {
			return true;
		}
		return false;
	}
	
	public static Iterable<Document> getAlltweets(){
		openTweetsConnection();
		FindIterable<Document> result = tweetsCollection.find();
		return result;
	}
	
	public static Long getCountTweets(){
		openTweetsConnection();
		Long result = tweetsCollection.count();
		return result;
	}
	
	public static Iterable<Document> getTweets(Long from, Long to){
		openTweetsConnection();
		Long limit = to - from;
		FindIterable<Document> result = tweetsCollection.find().limit(limit.intValue()).skip(from.intValue());
		return result;
	}
	
	public static Iterable<Document> getAllArticles(){
		openArticlesConnection();
		FindIterable<Document> result = articlesCollection.find();
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
	
	public static KeywordDTO getKeywordReport(String keyword, Date time, boolean byMonth) {
		openReportsConnection();
		KeywordDTO key = new KeywordDTO(keyword, time, 0D);
		FindIterable<Document> result = reportsCollection.find(getFilterForKeyword(key, byMonth));
		if(result != null && result.first() != null) {
			Document doc = result.first();
			ObjectMapper mapper = new ObjectMapper();
			KeywordDTO dto;
			try {
				dto = mapper.readValue(doc.toJson(), KeywordDTO.class);
				return dto;
			} catch (Exception e) {
				System.err.println("Error, cannot convert to DTO");
				e.printStackTrace();
				return null;
			} 
		}
		return null;
	}
	
	public static ArticleRatingDTO getArticleRating(String articleA, String articleB) {
		openReportsConnection();
		ArticleRatingDTO filter = new ArticleRatingDTO(articleA, articleB, 0);
		FindIterable<Document> result = reportsCollection.find(getFilterForRating(filter));
		if(result != null && result.first() != null) {
			Document doc = result.first();
			ObjectMapper mapper = new ObjectMapper();
			ArticleRatingDTO dto;
			try {
				dto = mapper.readValue(doc.toJson(), ArticleRatingDTO.class);
				return dto;
			} catch (Exception e) {
				System.err.println("Error, cannot convert to DTO");
				e.printStackTrace();
				return null;
			} 
		}
		return null;
	}
	
	public static Iterable<Document> getAllArticleRatings() {
		openReportsConnection();
		Bson filterType = Filters.eq("report_type", "articleRating");
		return reportsCollection.find(filterType);
	}
	
	public static ArticleRatingDTO getRecommendation(String objectA, String objectB) {
		openRecommendationsConnection();
		ArticleRatingDTO filter = new ArticleRatingDTO(objectA, objectB, 0);
		FindIterable<Document> result = recommendationsCollection.find(getFilterForRecommendation(filter));
		if(result != null && result.first() != null) {
			Document doc = result.first();
			ObjectMapper mapper = new ObjectMapper();
			ArticleRatingDTO dto;
			try {
				dto = mapper.readValue(doc.toJson(), ArticleRatingDTO.class);
				return dto;
			} catch (Exception e) {
				System.err.println("Error, cannot convert to DTO");
				e.printStackTrace();
				return null;
			} 
		}
		return null;
	}
	private static Bson getFilterForRecommendation(ArticleRatingDTO recommend) {
		Bson filterArtA = Filters.eq("articleA", recommend.getArticleA());
		Bson filterArtB = Filters.eq("articleB", recommend.getArticleB());
		Bson filterAnd = Filters.and(filterArtA, filterArtB);
		return filterAnd;
	}
	
	private static Bson getFilterForRating(ArticleRatingDTO rating) {
		Bson filterArtA = Filters.eq("articleA", rating.getArticleA());
		Bson filterArtB = Filters.eq("articleB", rating.getArticleB());
		Bson filterType = Filters.eq("report_type", "articleRating");
		Bson filterAnd = Filters.and(filterArtA, filterArtB, filterType);
		return filterAnd;
	}
	
	private static Bson getFilterForKeyword(KeywordDTO key, boolean byMonth) {
		LocalDate date = key.getTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf = date.getDayOfYear();
		Bson filterKey = Filters.eq("keyword", key.getKeyword());
		Bson filterType;
		if(byMonth) {
			filterType = Filters.eq("report_type", "tweetMonth");
		} else {
			filterType = Filters.eq("report_type", "tweet");
		}
		Bson filterYear = Filters.eq("report_year", year);
		Bson filterDay = Filters.eq("report_day", dayOf);
		Bson filterAnd = Filters.and(filterKey, filterType, filterYear, filterDay);
		return filterAnd;
	}
}
