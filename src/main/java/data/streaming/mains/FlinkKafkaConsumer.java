package data.streaming.mains;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import data.streaming.db.MongoConnector;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class FlinkKafkaConsumer {


	public static void main(String... args) throws Exception {

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();


		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(), props));
		stream.print();
		
//		AllWindowFunction<String, String, TimeWindow> function = new AllWindowFunction<String, String, TimeWindow>() {
//
//			private static final long serialVersionUID = -7023217243741221341L;
//
//			@Override
//			public void apply(TimeWindow arg0, Iterable<String> arg1, Collector<String> arg2) throws Exception {
//				for (String string : arg1) {
//					arg2.collect(string);
//				}
//			}
//		};
		
		stream.filter(x -> Utils.isValid(x)).map(x -> MongoConnector.saveTweetOnDB(x));
		
		if (Utils.isDebug()) {
			stream.print();
		}
		
		// execute program
		env.execute("Twitter Streaming Consumer");
	}

}
