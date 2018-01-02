package data.common.mains;

import data.common.db.MongoConnector;
import data.scraping.batchs.ArticlesScrapingBatch;
import data.scraping.scraper.ArticlesScrapper;
import data.streaming.batchs.ArticlesTweetsBatch;

public class ArticlesRunnable implements Runnable{

	private static boolean workingInProgress = false;
	
	@Override
	public void run() {
		if(!workingInProgress) {
			workingInProgress = true;
			System.out.println("System start!");
			try {
				System.out.println("Tweets part");
				ArticlesTweetsBatch.calculateTweetsStats();
				System.out.println("Tweets part finish");
			} catch (Exception e) {
				System.out.println("Tweets part error");
				e.printStackTrace();
			}
			try {
				System.out.println("Monthly Tweets part");
				ArticlesTweetsBatch.calculateMonthTweetsStats();
				System.out.println("Monthly Tweets part finish");
			} catch (Exception e) {
				System.out.println("Monthly Tweets part error");
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
				ArticlesTweetsBatch.calculateArticlesRatings();
				System.out.println("Ratings part finish");
			} catch (Exception e) {
				System.out.println("Ratings part error");
				e.printStackTrace();
			}
			try {
				System.out.println("Recommender part");
				ArticlesTweetsBatch.calculateRecommendations();
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
			try {
				System.out.println("Articles Number Graph part");
				ArticlesScrapingBatch.calculateArticlesNumberGraph();
				System.out.println("New Articles finish");
			} catch (Exception e) {
				System.out.println("Articles Number Graph part error");
				e.printStackTrace();
			}
			workingInProgress = false;
		} else {
			System.out.println("The system is already working");
		}
	}
}
