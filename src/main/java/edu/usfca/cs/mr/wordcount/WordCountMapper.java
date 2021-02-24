package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        if (value.toString().startsWith("W") == false) {
            return;
        }
        if (value.toString().toLowerCase().endsWith("no post title")) {
            return;
        }
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken().replaceAll("[^a-zA-Z0-9@#]", "");
            if (token.startsWith("W")) {
                String s[] = token.split("", 2);
                if (s != null && s.length > 1) {
                    System.out.println(s[1].trim());
                    context.write(new Text(s[1]), new IntWritable(1));
                }
            }
        }
    }
}
