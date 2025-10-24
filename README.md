# Apache Beam w/Kafka POC

I had done a Kafka/Mastodon POC shortly before this. So after getting the sample code from the Beam website working (spam vs. ham SMSs), I thought I'd try connecting Beam to the Kafka server.

After many issues with different runners, I decided to fallback to the default runner. To make this work though, I had to set `max_num_records` to a specific value, otherwise data wouldn't get processed.

It was great to finally see data being written :)
