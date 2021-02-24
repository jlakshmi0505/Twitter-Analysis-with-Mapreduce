package edu.usfca.cs.mr.part_3_sentiment_analysis;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class HashTagSentimentAnalysisMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {
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
            Text hashTagName = new Text();
            int sentimentSum = 0;
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.startsWith("W")) {
                    getHashTags(hashTagName, sentimentSum, token,context);

                }
            }

        }

    private void getHashTags(Text hashTagName, int sentimentSum, String token, Context context) throws IOException, InterruptedException {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            String t = s[1];
            StringTokenizer it = new StringTokenizer(t);
            while (it.hasMoreTokens()) {
                String tok = it.nextToken().replaceAll("[^a-zA-Z0-9#]", "");
                if (tok != null && tok.startsWith("#")) {
                    String tag  = tok;
                    // is it contains hash tag
                    tok = tok.replace("#", "").trim();
                    if (dictionary.containsKey(tok)) {
                        hashTagName.set(tag);
                        sentimentSum += Long.parseLong(dictionary.get(tok));
                        context.write(new Text(hashTagName), new IntWritable(sentimentSum));
                        sentimentSum = 0; // resetting sentiment sum for ech hashtag
                    }
                    /*else{ // if not present in dic will assign 0 sentiment as neutral
                        hashTagName.set(tag);
                        context.write(new Text(hashTagName), new IntWritable(0));
                        sentimentSum = 0;
                    }*/
                }
            }
        }
    }
}
