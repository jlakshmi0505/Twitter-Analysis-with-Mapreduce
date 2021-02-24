package edu.usfca.cs.mr.part_5_final_project_amazon_reviews;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AmazonProduct implements Writable {
    private Text productId;
    private Text productTitle;
    private IntWritable totalCount;
    private IntWritable avgRating;

    public AmazonProduct() {
        this.productId = new Text();
        this.productTitle = new Text();
        this.totalCount = new IntWritable(0);
        this.avgRating = new IntWritable(0);
    }
    public AmazonProduct(Text productId, Text productTitle, IntWritable totalCount, IntWritable avgRating) {
        this.productId = new Text(productId);
        this.productTitle = productTitle;
        this.totalCount = totalCount;
        this.avgRating = avgRating;
    }


    public Text getProductId() {
        return productId;
    }

    public void setProductId(Text productId) {
        this.productId = productId;
    }

    public Text getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(Text productTitle) {
        this.productTitle = productTitle;
    }

    public IntWritable getAvgRating() {
        return avgRating;
    }

    public void setAvgRating(IntWritable avgRating) {
        int avgRat= (this.getAvgRating().get()+avgRating.get())/getTotalCount().get();
        this.avgRating = new IntWritable(avgRat);
    }

    public IntWritable getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(IntWritable totalCount) {
        int sum = this.getTotalCount().get() + totalCount.get();
        this.totalCount = new IntWritable(sum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            productId.write(dataOutput);
            productTitle.write(dataOutput);
            totalCount.write(dataOutput);
            avgRating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        productId.readFields (dataInput);
        productTitle.readFields(dataInput);
        totalCount.readFields (dataInput);
        avgRating.readFields(dataInput);
    }

    @Override
    public String toString() {
        if (avgRating.get() == 0) {
            return "Product Title: " + productTitle + "\n" + " Total Reviews count:" + totalCount + "\n";
        } else {
            return "Product Title: " + productTitle + "\n" + " Average Rating: " + avgRating + "\n";
        }

    }
}
