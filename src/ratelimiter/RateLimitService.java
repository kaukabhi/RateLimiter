/**
 * 
 * Implement Rate Limiter using the Sliding Window with Counters approach.
 * 
 * We will rate limit per minute instead of second. Reason for this approach is that it's easily scalable to millions and billions of users.
 * 
 * Say, if we accept no more than 10 requests per second and then throttles the requests. 
 * This requirement then translates to - no more than 600 requests per minute and 36000 per hour.
 * 
 * For the first request of every user, we create 60 slots in the hashmap with the count of requests to 0. 
 * 
 * 1. First we will use a HashMap to store the number of requests for every user.
 * 2. For every user, fill up the map with 60 fixed minute slots
 * 3. For every request timestamp we will truncate it to the closest rounded minute just passed
 * 	[a] if the current timestamp is within the current minute then
 * 		[i] we increment the counter for current minute if the current request is within the limit
 * 		[ii] we reject the request if it has exceeded the rate limit
 * 	[b] if the new timestamp has passed the current minute then create the new minute key and set the counter to 1
 * 	[c] if the current timestamp is in the next hour then delete the entries from the previous hour and create new set of 60 minute slots. Whenever we step into a new hour, all the minute slots are recycled. 
 * 
 * @author Abhishek Kaukuntla
 * 
 */

package ratelimiter;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentSkipListMap;

public class RateLimitService {

	private static final long MILLIS_IN_SECOND = 1000;
	private static final long MILLIS_IN_MIUTE = 60000;
	private static final long MILLIS_IN_HOUR = 3600000;
	private static final long MILLIS_IN_DAY = 86400000;
	
	private static final int RATE_LIMIT_PER_MIN = 3;
	
	private static final int RATE_LIMIT_PER_HOUR = 10;
//	private static int totalRequestsInHour = 0;
	
	private static Map<Long, ConcurrentSkipListMap<Long, Integer>> limitMap = new HashMap<Long, ConcurrentSkipListMap<Long, Integer>>();
	private static Map<Long, Integer> requestCounterHourly = new HashMap<Long, Integer>();
	
	public static void main(String[] args) {
		
		RateLimitService rls = new RateLimitService();
		rls.test();
	}
	
	private void test() {
		
		System.out.println("=============================");
		System.out.println(String.format("Max reqs in minute: %d | Max reqs in hour: %d", RATE_LIMIT_PER_MIN, RATE_LIMIT_PER_HOUR));
		System.out.println("=============================");
		
		Long userId = 1L;
		refreshMinuteSlots(userId, System.currentTimeMillis());
		
		long reqT = System.currentTimeMillis();
		int sleepTime = 10000;
		
		for (int i=0; i<6; i++) {
			reqT = System.currentTimeMillis();
			boolean isAllowed = isAllowed(reqT, 1L);
			System.out.println(String.format("Req at %d allowed? %s (totalRequestsInHour: %d) ", reqT, isAllowed, requestCounterHourly.get(userId)));
			
			//if (i > 4) {
//				sleepTime += 10000;
				sleepTime *= 2;
				sleep(sleepTime);
			//}
		}
		
		System.out.println("=============================");
		printTimes(limitMap.get(userId));
		System.out.println("=============================");
		
	}
	
	private void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	private boolean isAllowed(Long reqT, Long userId) {
		
		ConcurrentSkipListMap<Long, Integer> times = limitMap.get(userId);
		
		Long truncatedMin = truncate(reqT, ChronoUnit.MINUTES);
		
		if (times != null) {
			Long truncatedHour = truncate(reqT, ChronoUnit.HOURS);
			Long truncatedDay = truncate(reqT, ChronoUnit.DAYS);
			
			if ((truncatedMin - truncatedDay >= MILLIS_IN_DAY)
					|| (truncatedMin - truncatedHour >= MILLIS_IN_HOUR)) {
				
				refreshMinuteSlots(userId, reqT);
				//totalRequestsInHour = 0;
				requestCounterHourly.put(userId, 0);
			}
			
		} else {
			refreshMinuteSlots(userId, reqT);
			
		}
		
		times = limitMap.get(userId);
				
		if ((times.get(truncatedMin) >= RATE_LIMIT_PER_MIN) || (requestCounterHourly.getOrDefault(userId, 0) >= RATE_LIMIT_PER_HOUR)) {
			return false;
		}
		
		System.out.println(String.format("Putting reqT %d in the bucket %d", reqT, truncatedMin));
		times.put(truncatedMin, times.get(truncatedMin) + 1);
		limitMap.put(userId, times);
//		totalRequestsInHour++;
		requestCounterHourly.put(userId, requestCounterHourly.getOrDefault(userId, 0) + 1);
		
		return true;
	}
	
	
	/*
	 * Truncates the timestamp to the closest time unit.
	 * 
	 */
	private Long truncate(Long time, ChronoUnit unit) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    	Date date = new Date(time);
    	String dateStr = sdf.format(date);
    	
    	Instant instant = Instant.parse(dateStr);
    	Instant returnValue = instant.truncatedTo(unit);
    	
    	return returnValue.toEpochMilli();
	}
	
	/* Add new set of minute slots.
	 * 
	 * @param userId
	 * @param reqT
	 */
	private void refreshMinuteSlots(Long userId, Long reqT) {
		Long minofDay = truncate(reqT, ChronoUnit.HOURS); // start min of the hour
		
		ConcurrentSkipListMap<Long, Integer> times = new ConcurrentSkipListMap<Long, Integer>();
		
		for (int i=0; i<=59; i++) { // add rounded minutes in an hour to the map
			times.put(minofDay, 0);
			minofDay += MILLIS_IN_MIUTE;
		}
		
		limitMap.put(userId, times);
	}
	
	/*
	 * Prints the current requests numbers from the map.
	 */
	private void printTimes(ConcurrentSkipListMap<Long, Integer> times) {
		
		System.out.println("Current state of the times for user...");
		for (Long time : times.keySet()) {
			System.out.println(time + " : " + times.get(time));
		}
	}
}
