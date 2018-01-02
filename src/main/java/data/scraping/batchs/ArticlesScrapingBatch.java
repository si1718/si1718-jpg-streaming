package data.scraping.batchs;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;

import data.common.db.MongoConnector;
import data.scraping.db.MongoScraping;
import data.scraping.dto.ArticleNumberGraphDTO;

public class ArticlesScrapingBatch {

	
	public static void calculateArticlesNumberGraph() {
		Map<Integer, ArticleNumberGraphDTO> articlesYearNumber = new HashMap<>();
		Long index = 0L, count = 100L;
		Iterable<Document> articles;
		boolean findMore = true;
		while (findMore) {
			articles = MongoConnector.getArticles(index, count);
			for (Document doc:articles) {
				index += 1;
				if(doc != null && doc.containsKey("year")) {
					Integer year = doc.getInteger("year");
					if(year != null) {
						ArticleNumberGraphDTO value = articlesYearNumber.get(year);
						if(value == null) {
							value = new ArticleNumberGraphDTO(year, 0);
						}
						value.setNumber(value.getNumber() + 1);
						articlesYearNumber.put(year, value);
					}
				}
			}
			if(index >= count) {
				count = index + 100L;
			} else {
				findMore = false;
			}
		}
		articlesYearNumber.forEach((year , articleGraph) -> {
			System.out.println("Article Graph Year: " + year + " Number: " + articleGraph.getNumber());
			try {
				MongoScraping.saveArticleNumberGraphDTO(articleGraph);
			} catch (JsonProcessingException e) {
				System.out.println("Error at saving article number graph");
				e.printStackTrace();
			}
		});
	}
}
