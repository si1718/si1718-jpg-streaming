package data.streaming.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class TweetEntities {
	
	@JsonProperty("hashtags")
	private List<TweetHashtag> hashtags;

	public TweetEntities() {
		super();
	}

	public TweetEntities(List<TweetHashtag> hashtags) {
		super();
		this.hashtags = hashtags;
	}

	public List<TweetHashtag> getHashtags() {
		return hashtags;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hashtags == null) ? 0 : hashtags.hashCode());
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
		TweetEntities other = (TweetEntities) obj;
		if (hashtags == null) {
			if (other.hashtags != null)
				return false;
		} else if (!hashtags.equals(other.hashtags))
			return false;
		return true;
	}
}
