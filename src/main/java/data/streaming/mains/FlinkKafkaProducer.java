
package data.streaming.mains;

import java.util.Properties;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming.auxi.ValidTagsTweetEndpoIntinitializer;
import data.streaming.db.MongoConnector;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class FlinkKafkaProducer {

	private static final Integer PARALLELISM = 1;

	public static void main(String... args) throws Exception {

		TwitterSource twitterSource = new TwitterSource(LoggingFactory.getTwitterCredentias());
		
		Set<String> keywords = MongoConnector.getArticlesKeywords();

		// Establecemos el filtro
		twitterSource.setCustomEndpointInitializer(new ValidTagsTweetEndpoIntinitializer(keywords.toArray(new String[keywords.size()])));

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(PARALLELISM);

		// Añadimos la fuente y generamos el stream como la salida de las llamadas
		// asíncronas para salvar los datos en MongoDB
		DataStream<String> stream = env.addSource(twitterSource);

		Properties props = LoggingFactory.getCloudKarafkaCredentials();

		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration<String> config = FlinkKafkaProducer010
				.writeToKafkaWithTimestamps(stream, props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(),
						props);
		config.setWriteTimestampToKafka(false);
		config.setLogFailuresOnly(false);
		config.setFlushOnCheckpoint(true);
		
		if(Utils.isDebug()) {
			stream.print();
		}

		env.execute("Twitter Streaming Producer");
	}

}
