package edu.usfca.cs.mr.part_4_top_mentioned_user;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TopMentionedUserMapper
extends Mapper<LongWritable, Text, Text, TopUserMentionedEachYear> {
    //private final static Logger log = Logger.getLogger("TopMentionedUserMapper.class");


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        List<TwitterUser> twitterUsers = new ArrayList<>();
        Text year = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");

        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (token.startsWith("T")) {
                getYearForTwitterUser(year, token);
            } else if (token.startsWith("W")) {
                String s[] = token.split("", 2);
                getMentionsForTwitterUser(twitterUsers, s);
            }
        }
        if (year != null) {
            TopUserMentionedEachYear topUserMentionedEachYear = new TopUserMentionedEachYear(twitterUsers, year);
            context.write(new Text(year), topUserMentionedEachYear);
        }
    }

    private void getMentionsForTwitterUser(List<TwitterUser> twitterUsers, String[] s) {
        TwitterUser twitterUser;
        if (s != null && s.length > 1) {
            System.out.println(s[1].trim());
            String t = s[1];
            StringTokenizer it = new StringTokenizer(t);
            while (it.hasMoreTokens()){
                    String tok = it.nextToken();
                    if (tok != null && tok.startsWith("@") && tok.length() > 1){
                        tok = tok.replaceAll("[^a-zA-Z0-9@#]", "");
                        twitterUser = new TwitterUser(new Text(tok),new IntWritable(1));
                        twitterUsers.add(twitterUser);
                    }
                }
            }
    }

    private void getYearForTwitterUser(Text year, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            String t = s[1].trim();
            if (t != null) {
                String year_Str = getYear(t);
                year.set(year_Str);
            }
        }
    }

    private String getYear(String t) {
        String date[] = t.split(" ");
        String date1[] = date[0].split("-");
        String year = date1[0];
        return year;
    }
}
