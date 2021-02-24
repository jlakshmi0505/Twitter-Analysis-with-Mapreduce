package edu.usfca.cs.mr.part_5_final_project_amazon_reviews;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TopRatedAmazonProductReducer
extends Reducer<Text, AmazonProduct, Text, AmazonProduct> {
    private ConcurrentSkipListSet<AmazonProduct> countMap;
    @Override
    protected void setup(Context context) {
        countMap = new ConcurrentSkipListSet<AmazonProduct>(new Comparator<AmazonProduct>() {
            @Override
            public int compare(AmazonProduct o1, AmazonProduct o2) {
                if (o2.getAvgRating().get() >= o1.getAvgRating().get()){
                    return 1;
                }
                else{
                    return -1;
                }
            }
        });
    }

    @Override
    protected void reduce(
            Text key, Iterable<AmazonProduct> values, Context context) {
        AmazonProduct u = new AmazonProduct();
        for (AmazonProduct val : values) {
            Text prod = new Text(val.getProductTitle());
            u.setProductTitle(prod);
            Text t = new Text(val.getProductId());
            u.setProductId(t);
            u.setTotalCount(val.getTotalCount());
            u.setAvgRating(val.getAvgRating());
        }
        countMap.add(u);
        if (countMap.size() > 10) {
            countMap.pollLast();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (AmazonProduct amazonProduct : countMap) {
            context.write(new Text(amazonProduct.getProductId()), amazonProduct);
        }
    }
}
