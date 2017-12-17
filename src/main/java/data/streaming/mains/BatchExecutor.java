package data.streaming.mains;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import data.streamings.batchs.ArticlesTweetsBatch;

public class BatchExecutor {

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	private static Thread producerth;
	private static Thread consumerth;
	private static ScheduledFuture<?> batchHandler;

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
		
		producerth = new Thread(producer);
		consumerth = new Thread(consumer);
		batchHandler = scheduler.scheduleAtFixedRate(batch, 1, 12, TimeUnit.HOURS);
		
		producerth.start();
		consumerth.start();
		
		final Runnable monitor = new Runnable() {
			
			@Override
			public void run() {
				System.out.println("Time to check applications");
				System.out.println("The system is runing with " + Thread.activeCount() + " threads");
				if (!batchHandler.isCancelled()) {
					System.out.println("The batch is working");
				} else {
					System.out.println("The batch is not working :(");
					System.out.println("Restarting...");
					batchHandler.cancel(true);
					batchHandler = scheduler.scheduleAtFixedRate(batch, 1, 12, TimeUnit.HOURS);
					System.out.println("Restarted!");
				}
				long delay = batchHandler.getDelay(TimeUnit.HOURS);
				if (delay > 0L) {
					System.out.println("Next batch execution in " + delay + " hours");
				} else {
					delay = batchHandler.getDelay(TimeUnit.MINUTES);
					System.out.println("Next batch execution in " + delay + " minutes");
				}
				System.out.println("System checked!");
			}
		};
		
		final ScheduledFuture<?> monitorHandler = scheduler.scheduleAtFixedRate(monitor, 1, 60, TimeUnit.MINUTES);
	}
}
