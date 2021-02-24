package edu.usfca.cs.mr.part_2_top_hashtag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TopHashTagMapper
extends Mapper<LongWritable, Text, Text, HashTagByWeek> {
    private final static Logger log = Logger.getLogger("TopHashTagMapper.class");
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        List<HashTag> hashTags = new ArrayList<>();
        Text weekNum = new Text();
        String week_year = "";
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (token.startsWith("T")) {
                week_year = getWeek_Year(weekNum, token);
            } else if (token.startsWith("W")) {
                getHashTag(hashTags, token);
            }
        }
        if (weekNum != null && week_year !=null && week_year.length() > 0) {
            HashTagByWeek hashTagByWeek = new HashTagByWeek(hashTags, weekNum);
            context.write(new Text(weekNum), hashTagByWeek);
        }
    }

    private void getHashTag(List<HashTag> hashTags, String token) {
        HashTag hashTag;
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            System.out.println(s[1].trim());
            String t = s[1];
            StringTokenizer it = new StringTokenizer(t);
            while (it.hasMoreTokens()){
                    String tok = it.nextToken();
                    if (tok != null && tok.startsWith("#")){
                        hashTag = new HashTag(new Text(tok),new IntWritable(1));
                        hashTags.add(hashTag);
                    }
                }
            }
    }

    private String getWeek_Year(Text weekNum, String token) {
        String s[] = token.split("", 2);
        if (s != null && s.length > 1) {
            String t = s[1].trim();
            if (t != null) {
                String weekId = null;
                try {
                    weekId = getWeekNUm(t);
                    if (!weekId.equals("")){
                        weekNum.set(weekId);
                        return weekId;
                    }
                    else{
                        return "";
                    }

                } catch (ParseException e) {
                   // e.printStackTrace();
                }

            }
        }
        return "";
    }

    private String getWeekNUm(String t) throws ParseException {
        String date[] = t.split(" ");
        Calendar calendar = new GregorianCalendar();
        System.out.println(date[0]);
        try{
            Date trialTime = new SimpleDateFormat("yyyy-MM-dd").parse(date[0]);
            calendar.setTime(trialTime);
            int week_no = calendar.get(Calendar.WEEK_OF_YEAR);
            String date1[] = date[0].split("-");
            String year = date1[0];
            return week_no +"_"+year;
        }
        catch(Exception ex){
            return "";
        }

    }
}
