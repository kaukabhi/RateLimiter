# RateLimiter
Rate Limiter

 Implement Rate Limiter using the Sliding Window with Counters approach.
 
 We will rate limit per minute instead of second. Reason for this approach is that it's easily scalable to millions and billions of users.
 If needed, this could be made to work at the seconds level too.
  
 Say, if we accept no more than 10 requests per second and then throttles the requests. 
 This requirement then translates to - no more than 600 requests per minute and 36000 per hour.
