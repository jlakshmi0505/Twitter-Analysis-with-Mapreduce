package edu.usfca.cs.mr.part_3_sentiment_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class HashTagSentimentAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static Logger log = Logger.getLogger("TwitterUserSentimentAnalysisReducer.class");

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for (IntWritable val : values) {
            count += val.get();
        }
        context.write(new Text(key), new IntWritable(count));
    }

}

