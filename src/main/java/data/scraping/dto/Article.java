package data.scraping.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

public class Article {
	
	public static final String acceptedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890-";
	
	private String doi;
	private String idArticle;
	private String journal;
	private String title;
	private List<String> authors;
	private Integer year;
	private Integer volume;
	private Integer number;
	private Integer initPage;
	private Integer lastPage;
	private String comments;
	
	public static Document articleToDocument(Article article) {
		if(article.getTitle() == null) {
			System.out.println("cat");
		}
		Document doc = new Document("idArticle", article.getIdArticle())
                .append("doi", article.getDoi())
                .append("journal", article.getJournal())
                .append("title", article.getTitle())
                .append("year", article.getYear())
                .append("volume", article.getVolume())
                .append("number", article.getNumber())
                .append("initPage", article.getInitPage())
                .append("lastPage", article.getLastPage())
                .append("comments", article.getComments())
                .append("authors", article.getAuthors());
		List<Document> authors = new ArrayList<Document>();
		for(String author:article.getAuthors()) {
			Document authorDoc = new Document("name", author);
			authors.add(authorDoc);
		}
		doc.append("authors", authors);
		return doc;
	}
	
	public void regenerateIdArticle() {
		if(idArticle != null && !idArticle.isEmpty()) {
			return;
		}
		if(doi != null) {
			if(doi.contains("dx.doi.org")) {
				String finaldoi = "";
				String[] parts = doi.split("/");
				boolean found = false;
				for(int i = 0; i < parts.length; i++) {
					if(!found) {
						if(parts[i].contains("dx.doi.org")) {
							found = true;
						}
						continue;
					} else {
						finaldoi += "-" + parts[i];
					}
				}
				idArticle = finaldoi;
			} else {
				idArticle = doi.replaceAll("/", "-");
			}
		} else {
			idArticle = generateUniqueID(this);
		}
		idArticle = idArticle.toLowerCase();
		idArticle = cleanSpecialChars(idArticle);
	}

	public static String generateUniqueID(Article article) {
		String result = "";
		String[] nameP = article.getTitle().split(" ");
		for(String name:nameP) {
			if(!name.isEmpty()) {
				result += name;
			}
		}
		if(article.getJournal() != null) {
			String[] journalP = article.getJournal().split(" ");
			for(String name:journalP) {
				if(!name.isEmpty()) {
					result += name;
				}
			}
		}
		if(article.getYear() != null) {
			result += article.getYear();
		}
		return result.trim().replaceAll("/", "-");
	}
	
	public String cleanSpecialChars(String idArticle) {
		String result = "";
		for(char c:idArticle.toCharArray()) {
			if(acceptedChars.contains("" + c)) {
				result += c;
			}
		}
		return result;
	}
	
	public static Article completeData(Article article, Map<String, Object> data) {
		if(data.containsKey("doi")) {
			article.setDoi((String)data.get("doi"));
		}
		if(data.containsKey("initPage")) {
			article.setInitPage((Integer)data.get("initPage"));
		}
		if(data.containsKey("lastPage")) {
			article.setLastPage((Integer)data.get("lastPage"));
		}
		if(data.containsKey("number")) {
			article.setNumber((Integer)data.get("number"));
		}
		if(data.containsKey("volume")) {
			article.setVolume((Integer)data.get("volume"));
		}
		if(data.containsKey("year")) {
			article.setYear((Integer)data.get("year"));
		}
		article.regenerateIdArticle();
		return article;
	}
	
	@Override
	public String toString() {
		return "Article [doi=" + doi + ", idArticle=" + idArticle + ", journal=" + journal + ", title=" + title
				+ ", year=" + year + "]";
	}

	public void addAuthor(String author) {
		authors.add(author);
	}
	
	public String getDoi() {
		return doi;
	}
	public void setDoi(String doi) {
		this.doi = doi;
	}
	public String getIdArticle() {
		return idArticle;
	}
	public void setIdArticle(String idArticle) {
		this.idArticle = idArticle;
	}
	public String getJournal() {
		return journal;
	}
	public void setJournal(String journal) {
		this.journal = journal;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public List<String> getAuthors() {
		return authors;
	}
	public void setAuthors(List<String> authors) {
		this.authors = authors;
	}
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Integer getVolume() {
		return volume;
	}
	public void setVolume(Integer volume) {
		this.volume = volume;
	}
	public Integer getNumber() {
		return number;
	}
	public void setNumber(Integer number) {
		this.number = number;
	}
	public Integer getInitPage() {
		return initPage;
	}
	public void setInitPage(Integer initPage) {
		this.initPage = initPage;
	}
	public Integer getLastPage() {
		return lastPage;
	}
	public void setLastPage(Integer lastPage) {
		this.lastPage = lastPage;
	}
	public String getComments() {
		return comments;
	}
	public void setComments(String comments) {
		this.comments = comments;
	}
}