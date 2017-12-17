package data.streaming.dto;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class KeywordDTO {

	String keyword;
	Date time;
	Double statistic;

	/**
	 * Default constructor for json, don't use it.
	 */
	public KeywordDTO() {
		
	}
	
	public KeywordDTO(String key, Date time, Double statistic) {
		super();
		this.keyword = key;
		this.time = time;
		this.statistic = statistic;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String key) {
		this.keyword = key;
	}

	public Double getStatistic() {
		return statistic;
	}

	public void setStatistic(Double statistic) {
		this.statistic = statistic;
	}

	@JsonFormat (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	public Date getTime() {
		return time;
	}

	@JsonFormat (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	public void setTime(Date time) {
		this.time = time;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyword == null) ? 0 : keyword.hashCode());
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
		KeywordDTO other = (KeywordDTO) obj;
		if (keyword == null) {
			if (other.keyword != null)
				return false;
		} else if (!keyword.equals(other.keyword))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KeywordDTO [key=" + keyword + ", time=" + time + ", statistic=" + statistic + "]";
	}

}
