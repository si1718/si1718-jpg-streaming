package data.streaming.mains;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import data.streaming.db.MongoStreaming;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class FlinkKafkaConsumer {

	private static boolean stopped = true;
	
	public static void main(String... args) throws Exception {
		stopped = false;
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(), props));
		
		stream.filter(x -> Utils.isValid(x)).map(x -> {
			try {
				return MongoStreaming.saveTweetOnDB(x, true);
			} catch (Exception e) {
				stopped = true;
				throw e;
			}
		});
		
		if (Utils.isDebug()) {
			stream.print();
		}
		// execute program
		env.execute("Twitter Streaming Consumer");
	}

	public static boolean isStopped() {
		return stopped;
	}
}
