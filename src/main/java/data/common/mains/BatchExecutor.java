package data.common.mains;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import data.streaming.mains.FlinkKafkaConsumer;
import data.streaming.mains.FlinkKafkaProducer;

public class BatchExecutor {

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	private static Thread producerth;
	private static Thread consumerth;
	private static ScheduledFuture<?> batchHandler;
	private static long prevTotal;
    private static long prevFree;

	public static void main(String... args) {
		final Runnable batch = (Runnable) new ArticlesRunnable();
		
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
					System.err.println("Cannot start consumer");
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
				Runtime rt = Runtime.getRuntime();
				long total = rt.totalMemory();
		        long free = rt.freeMemory();
				long used = total - free;
	            long prevUsed = (prevTotal - prevFree);
	            System.out.println("Total Memory: " + total + ", Used: " + used + ", Free: " + free + ", Used since last: " + (used - prevUsed) + ", Free since last: " + (free - prevFree));
	            prevTotal = total;
	            prevFree = free;
				long delay = batchHandler.getDelay(TimeUnit.HOURS);
				if (!batchHandler.isCancelled() && delay > -3L) {
					System.out.println("The batch is working");
				} else {
					System.out.println("The batch is not working :(");
					System.out.println("Restarting...");
					batchHandler.cancel(true);
					batchHandler = scheduler.scheduleAtFixedRate(batch, 1, 12, TimeUnit.HOURS);
					System.out.println("Restarted!");
				}
				if (delay > 0L) {
					System.out.println("Next batch execution in " + delay + " hours");
				} else {
					delay = batchHandler.getDelay(TimeUnit.MINUTES);
					System.out.println("Next batch execution in " + delay + " minutes");
				}
				if(FlinkKafkaConsumer.isStopped() || !consumerth.isAlive()) {
					System.out.println("Consumer is not working, restarting...");
					if(consumerth.isAlive()) {
						consumerth.interrupt();
					}
					consumerth = new Thread(consumer);
					consumerth.start();
					System.out.println("Consumer restarted");
				} else {
					System.out.println("Consumer is working.");
				}
				if(!producerth.isAlive()) {
					System.out.println("Producer is not working, restarting...");
					if(producerth.isAlive()) {
						producerth.interrupt();
					}
					producerth = new Thread(producer);
					producerth.start();
					System.out.println("Producer restarted");
				} else {
					System.out.println("Producer is working.");
				}
				System.out.println("System checked!");
			}
		};
		
		prevTotal = 0;
	    prevFree = Runtime.getRuntime().freeMemory();
		
		scheduler.scheduleAtFixedRate(monitor, 1, 60, TimeUnit.MINUTES);
	}
}
