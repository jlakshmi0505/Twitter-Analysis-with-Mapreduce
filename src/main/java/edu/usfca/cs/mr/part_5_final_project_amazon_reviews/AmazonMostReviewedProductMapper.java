package edu.usfca.cs.mr.part_5_final_project_amazon_reviews;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class AmazonMostReviewedProductMapper
extends Mapper<LongWritable, Text, Text, AmazonProduct> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        AmazonProduct amazonProduct = null;
        String[] reviews = value.toString().split(",");
        if (reviews.length > 6) {
            String product_Id = reviews[3];
            String product_Title = reviews[6];
            product_Title = product_Title.replaceAll("[^a-zA-Z0-9@#]", "");
            amazonProduct = new AmazonProduct(new Text(product_Id), new Text(product_Title), new IntWritable(1), new IntWritable(0));
            context.write(new Text(reviews[3]), amazonProduct);
        }
    }
}
