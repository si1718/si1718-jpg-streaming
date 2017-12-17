package data.streaming.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class ArticleRatingDTO {

	String articleA;
	String articleB;
	Integer rating;
	
	public ArticleRatingDTO() {
		
	}	
	
	public ArticleRatingDTO(String articleA, String articleB, Integer rating) {
		super();
		this.articleA = articleA;
		this.articleB = articleB;
		this.rating = rating;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((articleA == null) ? 0 : articleA.hashCode());
		result = prime * result + ((articleB == null) ? 0 : articleB.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArticleRatingDTO other = (ArticleRatingDTO) obj;
		if (articleA == null) {
			if (other.articleA != null)
				return false;
		} else if (!articleA.equals(other.articleA))
			return false;
		if (articleB == null) {
			if (other.articleB != null)
				return false;
		} else if (!articleB.equals(other.articleB))
			return false;
		return true;
	}

	public String getArticleA() {
		return articleA;
	}

	public void setArticleA(String articleA) {
		this.articleA = articleA;
	}

	public String getArticleB() {
		return articleB;
	}

	public void setArticleB(String articleB) {
		this.articleB = articleB;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "ArticleRatingDTO [articleA=" + articleA + ", articleB=" + articleB + ", rating=" + rating + "]";
	}
}
