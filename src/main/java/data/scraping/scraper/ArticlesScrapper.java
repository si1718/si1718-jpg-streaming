package data.scraping.scraper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import data.scraping.db.MongoScraping;
import data.scraping.dto.Article;

public class ArticlesScrapper {
	
	private static final String URL_BASE = "https://investigacion.us.es";
	private static final String URL_DEPARTAMENTS = "https://investigacion.us.es/sisius/sis_dep.php";
	private static final String H5_TEST = "PUBLICACIONES EN REVISTAS";
	
	private static Boolean isFrist;
	
	public static void crawDepartamentList() throws MalformedURLException, IOException {
		Document doc = Jsoup.parse(new URL(URL_DEPARTAMENTS), 10000);
		Elements table = doc.select("table.datatab");
		if(table != null && table.size() > 0) {
			Element departaments= table.first();
			Elements links = departaments.getElementsByTag("a");
			for (Element link:links) {
				if(!"center".equals(link.parentNode().nodeName())) {
					String departametL = URL_BASE + link.attr("href");
					crawDepartament(departametL);
				}
			}
		}
	}
	
	public static void crawDepartament(String departamentUrl) throws MalformedURLException, IOException {
		Document doc = Jsoup.parse(new URL(departamentUrl), 10000);
		Elements groups = doc.getElementsByTag("h5");
		for(Element group:groups) {
			Element par = group.nextElementSibling();
			if(par != null && "p".equals(par.tagName())) {
				Elements persons = par.getElementsByTag("a");
				if(persons != null && persons.size() > 0) {
					for (Element link : persons) {
						String personL = link.attr("href");
						if(personL.contains("idpers=")) {
							crawPerson(URL_BASE + personL);
						}
					}
				}
			}
		}
	}
	
	public static void crawPerson(String personUrl) throws MalformedURLException, IOException {
		Document doc = Jsoup.parse(new URL(personUrl), 10000);
		Elements h3Elements = doc.getElementsByTag("h3");
		String person = null;
		for(Element h3:h3Elements) {
			if(h3.text().contains("Ficha personal -")) {
				String[] text = h3.text().split("-");
				person = text[1];
				break;
			}
		}
		Elements elements = doc.getElementsByTag("h5");
		Element articlesNode = null;
		for (Element element : elements) {
			if(element == null) {
				System.out.println("ES NULL");
			}
			for(Node node:element.childNodes()){
				if(node instanceof TextNode){
					TextNode textNode = (TextNode)node;
					if(H5_TEST.equals(textNode.getWholeText().toUpperCase())) {
						System.out.println("FOUND");
						articlesNode = element;
						break;
					}
				}
			}
			if(articlesNode != null) {
				break;
			}
		}
		if(articlesNode == null) {
			System.out.println("PERSON " + person + " FIN, NO articles: ");
		}
		if(articlesNode != null) {
			Element sibling = articlesNode.nextElementSibling();
			Map<String, String> articles = new HashMap<String, String>();
			List<Article> articlesList = new ArrayList<Article>();
			Article tempArticle = null;
			String auxName = null;
			String auxText = "";
			do {
				if(sibling == null) {
					System.out.println("ES NULL");
				}
				if("h5".equals(sibling.tagName())) {
					if(auxName != null) {
						articles.put(auxName, auxText);
						tempArticle.regenerateIdArticle();
						articlesList.add(tempArticle);
					}
					break;
				}
				if("u".equals(sibling.tagName())) {
					if(auxName != null) {
						articles.put(auxName, auxText);
						tempArticle.regenerateIdArticle();
						articlesList.add(tempArticle);
						auxText = "";
					}
					auxName = sibling.text();
					tempArticle = new Article();
					tempArticle.setAuthors(extractAuthorsList(auxName));
				} else {
					if("br".equals(sibling.tagName())) {
						Node outerNode = sibling.nextSibling();
						if(outerNode instanceof TextNode){
							TextNode outerText = (TextNode)outerNode;
							if(tempArticle.getTitle() == null || tempArticle.getTitle().isEmpty()) {
								tempArticle.setTitle(extractArticleName(outerText.text()));
							}
							auxText += " " + outerText.text();
						}
					} else if("em".equals(sibling.tagName())) {
						auxText += " " + sibling.text();
						tempArticle.setJournal(extractJournalName(sibling.text()));
						Node outerNode = sibling.nextSibling();
						if(outerNode instanceof TextNode){
							TextNode outerText = (TextNode)outerNode;
							auxText += " " + outerText.text();
							Article.completeData(tempArticle, extractExtraData(outerText.text()));
						}
					}
				}
				sibling = sibling.nextElementSibling();
			} while(sibling != null);
			System.out.println("PERSON " + person + " FIN, Articles: " + articlesList.size()); 
			List<Article> filterList = new ArrayList<Article>();
			for(Article art:articlesList) {
				if(art.getIdArticle() == null || art.getIdArticle().isEmpty()) {
					System.out.println("ERROR, article has no id: " + art);
				} else {
					boolean out = false;
					for(Article irt:filterList) {
						if(irt.getIdArticle().equals(art.getIdArticle())) {
							out = true;
							break;
						}
					}
					if(!out && !MongoScraping.existArticle(art)) {
						if(!isFirstTime()) {
							if(!MongoScraping.existNewArticle(art)) {
								filterList.add(art);
							}
						} else {
							filterList.add(art);
						}
					}
				}
			}
			if(isFirstTime()) {
				MongoScraping.insertListOfArticles(filterList);
			} else {
				MongoScraping.insertListOfNewArticles(filterList);
			}
		}
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException {
		crawDepartamentList();
	}
	
	public static List<String> extractAuthorsList(String raw){
		List<String> result = new ArrayList<String>();
		raw = raw.replace(":", "");
		String[] parts = raw.split(",");
		String author = "";
		boolean surname = true;
		for(int i = 0; i < parts.length; i++) {
			if(surname) {
				author = parts[i];
			} else {
				result.add(author + "," + parts[i]);
			}
			surname = !surname;
		}
		return result;
	}
	
	public static String extractJournalName(String raw) {
		String name = raw.substring(4).trim();
		return name;
	}
	
	public static String extractArticleName(String raw) {
		String name = raw.trim();
		return name;
	}
	
	public static Map<String, Object> extractExtraData(String raw) {
		Map<String, Object> result = new HashMap<String, Object>();
		String[] parts = raw.split(" ");
		for(int i = 0; i < parts.length; i++) {
			if("Vol.".equals(parts[i].trim())) {
				String volumen = parts[i + 1].trim().replace(".", "");
				if(!"TBD".equals(volumen)) {
					try {
						result.put("volumen", Integer.valueOf(volumen));
					} catch (NumberFormatException e) {
						System.out.println("Cannot convert volumen number, is not a number");
					}
				}
				i += 1;
				continue;
			}
			if("Núm.".equals(parts[i].trim())) {
				String number = parts[i + 1].trim().replace(".", "");
				if(!"TBD".equals(number)) {
					try {
						result.put("number", Integer.valueOf(number));
					} catch (NumberFormatException e) {
						System.out.println("Cannot convert 'number' number, is not a number");
					}
				}
				i += 1;
				continue;
			}
			if("Pag.".equals(parts[i].trim())) {
				String[] pages = parts[i + 1].trim().replace(".", "").split("-");
				try {
					result.put("initPage", Integer.valueOf(pages[0]));
					if(pages.length > 1) {
						result.put("lastPage", Integer.valueOf(pages[1]));
					}
				} catch (NumberFormatException e) {
					System.out.println("Cannot convert pages number, uknown format");
					result.put("pages", parts[i + 1].trim());
				}
				i += 1;
				continue;
			}
			if(parts[i].trim().length() == 5 && !result.containsKey("year")) {
				String year = parts[i].trim().replace(".", "");
				year = year.replace(",", "");
				try {
					result.put("year", Integer.valueOf(year));
				} catch (NumberFormatException e) {
					System.out.println("Cannot convert year number, uknown format");
				}
				continue;
			}
			if(".".equals(parts[i].trim())) {
				//Ignore
				continue;
			}
			if(parts[i].contains("(") || parts[i].contains(")")) {
				//Ignore
				continue;
			}
			String doi = parts[i].trim();
			result.put("doi", doi);
		}
		return result;
	}
	
	public static boolean isFirstTime() {
		if(isFrist != null) {
			return isFrist;
		}
		String debug = System.getenv("SCRAP_FIRST");
		if(debug != null && Boolean.valueOf(debug)) {
			isFrist = true;
		} else {
			isFrist = false;
		}
		return isFrist;
	}
}
