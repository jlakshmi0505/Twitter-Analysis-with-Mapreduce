package edu.usfca.cs.mr.part_1_user_withmost_tweets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class UserWithMostTweetsReducer extends Reducer<Text, TwitterUser, Text, TwitterUser> {
    private ConcurrentSkipListSet<TwitterUser> countMap;
   // private final static Logger log = Logger.getLogger("UserWithMostTweetsReducer.class");

    @Override
    protected void setup(Context context) {
        countMap = new ConcurrentSkipListSet<>(new Comparator<TwitterUser>() {
            @Override
            public int compare(TwitterUser o1, TwitterUser o2) {
                if (o2.getTotalCount().get() >= o1.getTotalCount().get()) {
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
            u.setTimestamp(val.getTimestamp());
            Text t = new Text(val.getUserName());
            u.setUserName(t);
            u.setTotalCount(val.getTotalCount());
        }
        countMap.add(u);
        if (countMap.size() > 5) {
            countMap.pollLast();
        }

    }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (TwitterUser twitterUser : countMap) {
                context.write(new Text(twitterUser.getUserName()), twitterUser);
            }
        }
    }

