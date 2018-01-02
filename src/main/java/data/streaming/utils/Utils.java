package data.streaming.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Locale;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import data.scraping.dto.Article;
import data.scraping.dto.ArticleNumberGraphDTO;
import data.streaming.dto.KeywordDTO;
import data.streaming.dto.TweetDTO;

public class Utils {
	
	//public static final String[] TAGNAMES = { "#OTDirecto12D", "#DefendemosLosAyuntamientos" };
	private static final ObjectMapper mapper = new ObjectMapper();
	
	private static Boolean isDebug = null;

	public static TweetDTO createTweetDTO(String x) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {

		}
		return result;
	}
	
	public static String convertTweetToPublicationsFormat(String json, boolean clean) {
		TweetDTO tweet = createTweetDTO(json);
		if(tweet == null) {
			System.err.println("Cannot convert tweet to publications format");
			return null;
		}
		if(clean) {
			tweet = cleanTweetFromUnnecessaryData(tweet);
		}
		try {
			return mapper.writeValueAsString(tweet);
		} catch (JsonProcessingException e) {
			System.err.println("Cannot convert tweet to publications format");
			e.printStackTrace();
		}
		return null;
	}
	
	public static TweetDTO cleanTweetFromUnnecessaryData(TweetDTO tweet) {
		return new TweetDTO(tweet.getCreatedAt(), null, tweet.getText(), null);
	}
	
	public static boolean isValid(String x) {
		return createTweetDTO(x) != null;
	}
	
	public static boolean isDebug() {
		if(isDebug != null) {
			return isDebug;
		}
		String debug = System.getenv("EXEC_DEBUG");
		if(debug != null && Boolean.valueOf(debug)) {
			isDebug = true;
		} else {
			isDebug = false;
		}
		return isDebug;
	}
	
	public static Date formatTwitterDate(String date) {
		SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
		try {
			return formatter.parse(date);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static KeywordDTO convertJsonToKeywordDTO(String json) {
		ObjectMapper mapper = new ObjectMapper();
		KeywordDTO dto;
		try {
			dto = mapper.readValue(json, KeywordDTO.class);
			return dto;
		} catch (Exception e) {
			System.err.println("Error, cannot convert to DTO");
			e.printStackTrace();
			return null;
		}
	}
	
	public static ArticleNumberGraphDTO convertJsonToArticlesNumberGraphDTO(String json) {
		ObjectMapper mapper = new ObjectMapper();
		ArticleNumberGraphDTO dto;
		try {
			dto = mapper.readValue(json, ArticleNumberGraphDTO.class);
			return dto;
		} catch (Exception e) {
			System.err.println("Error, cannot convert to DTO");
			e.printStackTrace();
			return null;
		}
	}
	
	public static Article convertJsonToArticleDTO(String json) {
		ObjectMapper mapper = new ObjectMapper();
		Article dto;
		try {
			dto = mapper.readValue(json, Article.class);
			return dto;
		} catch (Exception e) {
			System.err.println("Error, cannot convert to DTO");
			e.printStackTrace();
			return null;
		}
	}
	
	public static Integer extractDayFromDate(Date date) {
		LocalDate locdate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer dayOf = locdate.getDayOfYear();
		return dayOf;
	}
	
	public static Integer extractYearFromDate(Date date) {
		LocalDate locdate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Integer year = locdate.getYear();
		return year;
	}
}
