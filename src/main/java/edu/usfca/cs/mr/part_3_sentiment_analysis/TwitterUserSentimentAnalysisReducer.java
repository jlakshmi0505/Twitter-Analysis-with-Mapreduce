package edu.usfca.cs.mr.part_3_sentiment_analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TwitterUserSentimentAnalysisReducer extends Reducer<Text, TwitterUser, Text, TwitterUser> {
    private ConcurrentSkipListSet<TwitterUser> countMap;
    private final static Logger log = Logger.getLogger("TwitterUserSentimentAnalysisReducer.class");
    @Override
    protected void setup(Context context) {
        countMap = new ConcurrentSkipListSet<>(new Comparator<TwitterUser>() {
            @Override
            public int compare(TwitterUser o1, TwitterUser o2) {
                if (o2.getSentimentCount().get() >= o1.getSentimentCount().get()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
    }

    @Override
    protected void reduce(
            Text key, Iterable<TwitterUser> values, Context context) {
        TwitterUser u = new TwitterUser();
        for (TwitterUser val : values) {
            u.setTweets(val.getTweets());
            Text t = new Text(val.getUserName());
            u.setUserName(t);
            u.setSentimentCount(val.getSentimentCount());
        }
        countMap.add(u);
        if (countMap.size() > 10) {
            countMap.pollFirst();
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (TwitterUser twitterUser : countMap) {
            Text userName = twitterUser.getUserName();
            context.write(new Text(userName), twitterUser);
        }
    }
    }

