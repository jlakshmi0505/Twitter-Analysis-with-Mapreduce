package edu.usfca.cs.mr.part_2_top_hashtag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HashTag implements Writable{
    private Text hashTagName;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HashTag)) return false;
        HashTag hashTag = (HashTag) o;
        return getHashTagName().equals(hashTag.getHashTagName());
    }

    @Override
    public int hashCode() {
        return 1;
    }

    private IntWritable totalCount;

    public HashTag() {
        this.hashTagName = new Text();
        this.totalCount = new IntWritable(0);
    }
    public HashTag(Text hashTagName,IntWritable totalCount) {
        this.hashTagName = new Text(hashTagName);
        this.totalCount = totalCount;
    }
    public Text getHashTagName() {
        return hashTagName;
    }

    public void setHashTagName(Text hashTagName) {
        this.hashTagName = hashTagName;
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
            hashTagName.write(dataOutput);
            totalCount.write(dataOutput);

        }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        hashTagName.readFields (dataInput);
        totalCount.readFields (dataInput);
    }

    @Override
    public String toString() {
        return "HashTagName :" + "->" +  hashTagName + " " + "TotalCount :" + "->" + totalCount ;
    }
}
