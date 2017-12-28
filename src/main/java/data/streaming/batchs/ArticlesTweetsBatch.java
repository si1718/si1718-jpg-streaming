package data.streaming.batchs;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.scored.ScoredId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;

import data.common.db.MongoConnector;
import data.scraping.scraper.ArticlesScrapper;
import data.streaming.db.MongoStreaming;
import data.streaming.dto.ArticleRatingDTO;
import data.streaming.dto.KeywordDTO;
import data.streaming.recommender.Recom;
import data.streaming.utils.Utils;

public class ArticlesTweetsBatch implements Runnable{

	private static final int MAX_RECOMMENDATIONS = 5;
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
				System.out.println("Recovering DB free space");
				MongoConnector.repairDatabase();
				System.out.println("Recovered");
			} catch (Exception e) {
				System.err.println("Error while recovering DB free space");
			}
			try {
				System.out.println("Ratings part");
				calculateArticlesRatings();
				System.out.println("Ratings part finish");
			} catch (Exception e) {
				System.out.println("Ratings part error");
				e.printStackTrace();
			}
			try {
				System.out.println("Recommender part");
				calculateRecommendations();
				System.out.println("Recommender part finish");
			} catch (Exception e) {
				System.out.println("Recommender part error");
				e.printStackTrace();
			}
			try {
				System.out.println("New Articles part");
				ArticlesScrapper.crawDepartamentList();
				System.out.println("New Articles finish");
			} catch (Exception e) {
				System.out.println("New Articles part error");
				e.printStackTrace();
			}
			workingInProgress = false;
		} else {
			System.out.println("The system is already working");
		}
	}
	
	public static KeywordDTO getKeywordDTOFromMap(String keyword, Date createdAt, boolean byMonth, Map<Integer, Map<Integer, Map<String, KeywordDTO>>> map) {
		LocalDate date = createdAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf;
		if(byMonth) {
			dayOf = date.getMonthValue();
		} else {
			dayOf = date.getDayOfYear();
		}
		
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
	
	public static void setKeywordDTOToMap(KeywordDTO keyword, boolean byMonth, Map<Integer, Map<Integer, Map<String, KeywordDTO>>> map) {
		LocalDate date = keyword.getTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = date.getYear();
		Integer dayOf;
		if(byMonth) {
			dayOf = date.getMonthValue();
		} else {
			dayOf = date.getDayOfYear();
		}
		
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
		MongoStreaming.deleteNullTweets();
		Set<String> keywords = MongoStreaming.getArticlesKeywords();
		Map<Integer, Map<Integer, Map<String, KeywordDTO>>> allStatics = new HashMap<>();
		Map<Integer, Map<Integer, Map<String, KeywordDTO>>> monthStatics = new HashMap<>();
		List<String> tweetsToDelete = new ArrayList<String>();
		Long tweetsCount = MongoStreaming.getCountTweets();
		Long tweetNum = 0L;
		Long limit = 100L;
		
		while (tweetNum < tweetsCount) {
			if(!tweetsToDelete.isEmpty()) {
				MongoStreaming.deleteListTweet(tweetsToDelete);
				tweetsToDelete.clear();
			}
			
			if((tweetNum + 100L) > tweetsCount) {
				limit = tweetsCount - tweetNum;
			}
			
			Iterable<Document> result = MongoStreaming.getTweets(0L, limit);
			for (Document doc:result) {
				tweetNum += 1;
				String tweet = doc.getString("text");
				if(tweet == null || tweet.isEmpty()) {
					continue;
				}
				try {
					for(String keyword:keywords){
						if(tweet.contains(keyword)) {
							Date createdAt = Utils.formatTwitterDate((String)doc.get("created_at"));
							KeywordDTO valor = getKeywordDTOFromMap(keyword, createdAt, false, allStatics);
							valor.setStatistic(valor.getStatistic() + 1D);
							setKeywordDTOToMap(valor, false, allStatics);
							valor = getKeywordDTOFromMap(keyword, createdAt, true, monthStatics);
							valor.setStatistic(valor.getStatistic() + 1D);
							setKeywordDTOToMap(valor, true, monthStatics);
						}
					}
				} catch (Exception e) {
					System.out.println("Error while calculating tweet stats");
					e.printStackTrace();
				}finally {
					tweetsToDelete.add(tweet);
				}
			}
		}
		
		if(!tweetsToDelete.isEmpty()) {
			MongoStreaming.deleteListTweet(tweetsToDelete);
			tweetsToDelete.clear();
		}
		
		allStatics.forEach((year , mapDays) -> {
			mapDays.forEach((dayOf, mapKeys) -> {
				mapKeys.forEach((keyword, dto) -> {
					System.out.println("Year: " + year + " Day: " + dayOf + " Keyword: " + keyword + " Value: " + dto.getStatistic());
					try {
						MongoStreaming.saveReport(dto, false);
					} catch (JsonProcessingException e) {
						System.out.println("Error at saving report");
						e.printStackTrace();
					}
				});
			});
		});
		monthStatics.forEach((year , mapDays) -> {
			mapDays.forEach((month, mapKeys) -> {
				mapKeys.forEach((keyword, dto) -> {
					System.out.println("Year: " + year + " Month: " + month + " Keyword: " + keyword + " Value: " + dto.getStatistic());
					try {
						MongoStreaming.saveReport(dto, true);
					} catch (JsonProcessingException e) {
						System.out.println("Error at saving report");
						e.printStackTrace();
					}
				});
			});
		});
	}
	
	@SuppressWarnings("unchecked")
	public void calculateArticlesRatings(){
		Iterable<Document> documents = MongoStreaming.getAllArticles();
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
									Double rawRating = (coincidences * 5) / (1D * artKey.size());
									Double rawReverseRating = (coincidences * 5) / (1D * artBKey.size());
									Integer rating = (int) Math.round(rawRating);
									Integer reverseRating = (int) Math.round(rawReverseRating);
									ArticleRatingDTO articleRating = new ArticleRatingDTO(idArticleA, idArticleB, rating);
									ArticleRatingDTO articleReverseRating = new ArticleRatingDTO(idArticleB, idArticleA, reverseRating);
									ratings.add(articleRating);
									ratings.add(articleReverseRating);
								}
							}
						}
						articles.add(article);
					}
				}
			}
		}
		System.out.println("Found " + ratings.size() + " ratings!" );
		MongoStreaming.saveRatings(ratings);
	}
	
	public static void calculateRecommendations() {
		Iterable<Document> documents = MongoStreaming.getAllArticleRatings();
		Set<ArticleRatingDTO> articlesRatings = new HashSet<ArticleRatingDTO>();
		for(Document doc:documents) {
			if(doc != null) {
				String articleA = doc.getString("articleA");
				String articleB = doc.getString("articleB");
				Double points = doc.getDouble("rating");
				ArticleRatingDTO dto = new ArticleRatingDTO(articleA, articleB, points);
				articlesRatings.add(dto);
			}
		}
		try {
			Map<String, Long> keys = Maps.asMap(articlesRatings.stream().map((ArticleRatingDTO x) -> x.getArticleA()).collect(Collectors.toSet()), (String y) -> new Long(y.hashCode()));
			Map<Long, String> reverse = new HashMap<>();
			keys.forEach((x, y) -> reverse.put(y, x));
			ItemRecommender recom = Recom.getRecommender(articlesRatings);
			for (String key:keys.keySet()) {
				List<ScoredId> recommendations = recom.recommend(keys.get(key), MAX_RECOMMENDATIONS);
				for(ScoredId score:recommendations) {
					Long keyB = score.getId();
					String objectB = reverse.get(keyB);
					if(key.equals(objectB)) {
						continue;
					}
					Double ss = score.getScore();
					try {
						ArticleRatingDTO rating = new ArticleRatingDTO(key, objectB, ss);
						MongoStreaming.saveRecommendation(rating);
					} catch (Exception e) {
						System.err.println("Error while saving recommendation.");
						e.printStackTrace();
					}
				}
			}
		} catch (RecommenderBuildException e) {
			System.err.println("Cannot get recommender");
			e.printStackTrace();
		}
	}
}
