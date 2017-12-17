package data.streaming.mains;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import data.streamings.batchs.ArticlesTweetsBatch;

public class BatchExecutor {

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public static void main(String... args) {
		final Runnable batch = (Runnable) new ArticlesTweetsBatch();
		
		final Runnable producer = new Runnable() {
			
			@Override
			public void run() {
				try {
					FlinkKafkaProducer.main(args);
				} catch (Exception e) {
					System.err.println("Cannot start producer");
					e.printStackTrace();
				}
			}
		};
		
		final Runnable consumer = new Runnable() {
			
			@Override
			public void run() {
				try {
					FlinkKafkaConsumer.main(args);
				} catch (Exception e) {
					System.err.println("Cannot start producer");
					e.printStackTrace();
				}
			}
		};
		
		final ScheduledFuture<?> batchHandler = scheduler.scheduleAtFixedRate(batch, 15, 15, TimeUnit.SECONDS);
		Thread producerth = new Thread(producer);
		Thread consumerth = new Thread(consumer);
		producerth.start();
		consumerth.start();
	}
}
