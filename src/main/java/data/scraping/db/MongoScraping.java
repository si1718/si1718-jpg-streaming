package data.scraping.db;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import data.common.db.MongoConnector;
import data.scraping.dto.Article;

public class MongoScraping {
	
	private static MongoCollection<Document> articlesCollection;
	private static MongoCollection<Document> newArticlesCollection;
	
	
	public static void openArticlesConnection() {
		if(MongoScraping.articlesCollection != null) {
			return;
		}
		MongoConnector.openConnection();
		MongoCollection<Document> collection = MongoConnector.getDatabase().getCollection("articles");
		MongoScraping.articlesCollection = collection;
	}
	
	public static void openNewArticlesConnection() {
		if(MongoScraping.newArticlesCollection != null) {
			return;
		}
		MongoConnector.openConnection();
		MongoCollection<Document> collection = MongoConnector.getDatabase().getCollection("newArticles");
		MongoScraping.newArticlesCollection = collection;
	}
	
	public static void insertListOfArticles(List<Article> articlesList) {
		openArticlesConnection();
		insertListOfArticles(articlesList, articlesCollection);
	}
	
	public static void insertListOfNewArticles(List<Article> articlesList) {
		openNewArticlesConnection();
		insertListOfArticles(articlesList, newArticlesCollection);
	}
	
	private static void insertListOfArticles(List<Article> articlesList, MongoCollection<Document> articlesCollection) {
		List<Document> insertList = new ArrayList<Document>();
		for(Article art:articlesList) {
			insertList.add(Article.articleToDocument(art));
		}
		if (insertList != null && !insertList.isEmpty()) {
			articlesCollection.insertMany(insertList);
		}
	}
	
	public static boolean existArticle(Article article) {
		openArticlesConnection();
		return existArticle(article, articlesCollection);
	}
	
	public static boolean existNewArticle(Article article) {
		openNewArticlesConnection();
		return existArticle(article, newArticlesCollection);
	}
	
	private static boolean existArticle(Article article, MongoCollection<Document> articlesCollection) {
		FindIterable<Document> result = articlesCollection.find(Filters.eq("idArticle", article.getIdArticle()));
		if(result != null && result.first() != null) {
			return true;
		}
		return false;
	}

}
