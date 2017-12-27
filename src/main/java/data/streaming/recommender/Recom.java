package data.streaming.recommender;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;

import data.streaming.dto.ArticleRatingDTO;

public class Recom {

	public static ItemRecommender getRecommender(Set<ArticleRatingDTO> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
	}

	private static Collection<? extends Event> createEventCollection(Set<ArticleRatingDTO> ratings) {
		List<Event> result = new LinkedList<>();

		for (ArticleRatingDTO dto : ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getArticleA().hashCode());
			r.setUserId(dto.getArticleB().hashCode());
			r.setRating(dto.getRating());
			result.add(r);
		}
		return result;
	}	
}
