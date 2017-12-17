package data.streamings.batchs;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import data.streaming.db.MongoConnector;
import data.streaming.dto.ArticleRatingDTO;
import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;

public class ArticlesTweetsBatch implements Runnable{

	private static boolean workingInProgress = false;
	
	private void initServices() {
		
	}
	
	@Override
	public void run() {
		initServices();
		if(!workingInProgress) {
			workingInProgress = true;
			System.out.println("System start!");
			try {
				System.out.println("Tweets part");
				calculateTweetsStats();
				System.out.println("Tweets part finish");
			} catch (Exception e) {
				System.out.println("Tweets part error");
				e.printStackTrace();
			}
			try {
				System.out.println("Ratings part");
				calculateArticlesRatings();
				System.out.println("Ratings part finish");
			} catch (Exception e) {
				System.out.println("Ratings part error");
				e.printStackTrace();
			}
			workingInProgress = false;
		} else {
			System.out.println("The system is already working");
		}
	}
	
	public static KeywordDTO getKeywordDTOFromMap(String keyword, Date createdAt, Map<Integer, Map<Integer, Map<String, KeywordDTO>>> map) {
		LocalDate date = createdAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf = date.getDayOfYear();
		Map<Integer, Map<String, KeywordDTO>> mapYear = map.get(year);
		if (mapYear == null) {
			mapYear = new HashMap<>();
			map.put(year, mapYear);
		}
		
		Map<String, KeywordDTO> mapDayOf = mapYear.get(dayOf);
		if(mapDayOf == null) {
			mapDayOf = new HashMap<>();
			mapYear.put(dayOf, mapDayOf);
		}
		
		KeywordDTO keywordDTO = mapDayOf.get(keyword);
		if(keywordDTO == null) {
			keywordDTO = new KeywordDTO(keyword, Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant()), 0D);
			mapDayOf.put(keyword, keywordDTO);
		}
		
		return keywordDTO;
	}
	
	public static void setKeywordDTOToMap(KeywordDTO keyword, Map<Integer, Map<Integer, Map<String, KeywordDTO>>> map) {
		LocalDate date = keyword.getTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf = date.getDayOfYear();
		Map<Integer, Map<String, KeywordDTO>> mapYear = map.get(year);
		if (mapYear == null) {
			mapYear = new HashMap<>();
			map.put(year, mapYear);
		}
		
		Map<String, KeywordDTO> mapDayOf = mapYear.get(dayOf);
		if(mapDayOf == null) {
			mapDayOf = new HashMap<>();
			mapYear.put(dayOf, mapDayOf);
		}
		
		mapDayOf.put(keyword.getKeyword(), keyword);
	}
	
	public void calculateTweetsStats() {
		Set<String> keywords = MongoConnector.getArticlesKeywords();
		Map<Integer, Map<Integer, Map<String, KeywordDTO>>> allStatics = new HashMap<>();		
		Iterable<Document> result = MongoConnector.getAlltweets();
		
		for (Document doc:result) {
			String tweet = doc.getString("text");
			if(tweet == null || tweet.isEmpty()) {
				continue;
			}
			try {
				for(String keyword:keywords){
					if(tweet.contains(keyword)) {
						Date createdAt = Utils.formatTwitterDate((String)doc.get("created_at"));
						KeywordDTO valor = getKeywordDTOFromMap(keyword, createdAt, allStatics);
						valor.setStatistic(valor.getStatistic() + 1D);
						setKeywordDTOToMap(valor, allStatics);
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println("ERRR");
				e.printStackTrace();
			}
		}
		
		allStatics.forEach((year , mapDays) -> {
			mapDays.forEach((dayOf, mapKeys) -> {
				mapKeys.forEach((keyword, dto) -> {
					System.out.println("Year: " + year + " Day: " + dayOf + " Keyword: " + keyword + " Value: " + dto.getStatistic());
					try {
						MongoConnector.saveReport(dto);
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						System.out.println("Error at saving report");
						e.printStackTrace();
					}
				});
			});
		});
	}
	
	@SuppressWarnings("unchecked")
	public void calculateArticlesRatings(){
		Iterable<Document> documents = MongoConnector.getAllArticles();
		List<Document> articles = new ArrayList<>();
		List<ArticleRatingDTO> ratings = new ArrayList<>();
		for(Document article:documents) {
			if(article.containsKey("idArticle") && article.containsKey("keywords")) {
				if(article.get("keywords") != null) {
					String idArticleA = (String) article.get("idArticle");
					List<String> artKey = (List<String>) article.get("keywords");
					if(artKey.size() > 0) {
						if(articles.size() > 0) {
							for (Document articleB:articles){
								String idArticleB = (String) articleB.get("idArticle");
								List<String> artBKey = (List<String>) articleB.get("keywords");
								Integer coincidences = 0;
								for(String keyword: artBKey) {
									if(artKey.contains(keyword)) {
										coincidences += 1;
									}
								}
								if(coincidences >= 1) {
									Integer min = Math.min(artKey.size(), artBKey.size());
									Double rawRating = (coincidences * 5) / (1D * min);
									Integer rating = (int) Math.round(rawRating);
									ArticleRatingDTO articleRating = new ArticleRatingDTO(idArticleA, idArticleB, rating);
									ratings.add(articleRating);
								}
							}
						}
						articles.add(article);
					}
				}
			}
		}
		System.out.println("Found " + ratings.size() + " ratings!" );
		MongoConnector.saveRatings(ratings);
	}
}
