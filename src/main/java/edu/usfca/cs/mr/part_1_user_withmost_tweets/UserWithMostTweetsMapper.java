package edu.usfca.cs.mr.part_1_user_withmost_tweets;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class UserWithMostTweetsMapper
extends Mapper<LongWritable, Text, Text, TwitterUser> {
    private final static Logger log = Logger.getLogger("UserWithMostTweetsMapper.class");
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        TwitterUser twitterUser = null;
        Text userName = new Text();
        List<Text> tweets = new ArrayList<>();
        List<Text> timeStamps = new ArrayList<>();
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (token.startsWith("T")) {
                getTimeStamp(timeStamps, token);
            } else if (token.startsWith("U")) {
                getUserName(userName, token);
            } else if (token.startsWith("W")) {
                getTweets(tweets, token);
            }
        }
        twitterUser = new TwitterUser(new Text(userName), timeStamps, tweets, new IntWritable(1));
        context.write(new Text(userName), twitterUser);
    }

    private void getTweets(List<Text> tweets, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            System.out.println(s[1]);
            String t = s[1];
            t=t.replaceAll("[^a-zA-Z0-9@#]", "");
            tweets.add(new Text(t));
        }
    }

    private void getUserName(Text userName, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            System.out.println(s[1]);
            String t = s[1];
            userName.set(t);
        }
    }

    private void getTimeStamp(List<Text> timeStamps, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            System.out.println(s[1]);
            String t = s[1];
            timeStamps.add(new Text(t));
        }
    }
}
