package edu.usfca.cs.mr.part_3_sentiment_analysis;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;

import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.*;
import java.util.logging.Logger;

import org.json.simple.parser.JSONParser;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TwitterUserSentimentAnalysisMapper
extends Mapper<LongWritable, Text, Text, TwitterUser> {
    //private final static Logger log = Logger.getLogger("TwitterUserSentimentAnalysisMapper.class");
    JSONParser parser = null;
    Map<String,String> dictionary = null;


    @Override
    public void setup(Context context) throws IOException {
        parser = new JSONParser();
        dictionary = new HashMap<String, String>();
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                String line = "";
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    dictionary.put(tokens[0], tokens[1]);
                }
            } catch (Exception e) {
                System.out.println("Unable to read the cached filed");
                System.exit(1);
            }
        }
    }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // tokenize into words.
            TwitterUser twitterUser = null;
            Text userName = new Text();
            List<Text> tweets = new ArrayList<>();
            int sentimentSum = 0;
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.startsWith("U")) {
                    getUserName(userName, token);
                } else if (token.startsWith("W")) {
                    sentimentSum = getTweets(tweets, sentimentSum, token);

                }
            }
            twitterUser = new TwitterUser(new Text(userName), tweets, new IntWritable(sentimentSum));
            context.write(new Text(userName), twitterUser);
        }

    private int getTweets(List<Text> tweets, int sentimentSum, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            String t = s[1];
            tweets.add(new Text(t));
            StringTokenizer it = new StringTokenizer(t);
            while (it.hasMoreTokens()) {
                String tok = it.nextToken().replaceAll("[^a-zA-Z0-9]", "");
                if (tok != null && dictionary.containsKey(tok)) {
                    sentimentSum += Long.parseLong(dictionary.get(tok));
                }
            }
        }
        return sentimentSum;
    }

    private void getUserName(Text userName, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            System.out.println(s[1]);
            String t = s[1];
            userName.set(t);
        }
    }
}
