package edu.usfca.cs.mr.part_4_top_mentioned_user;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwitterUser implements Writable{
    private Text userName;
    private IntWritable totalMentionedCount;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TwitterUser)) return false;
        TwitterUser twitterUser = (TwitterUser) o;
        return getUserName().equals(twitterUser.getUserName());
    }

    @Override
    public int hashCode() {
        return 1;
    }

    public Text getUserName() {
        return userName;
    }

    public void setUserName(Text userName) {
        this.userName = userName;
    }

    public TwitterUser() {
        this.userName = new Text();
        this.totalMentionedCount = new IntWritable(0);
    }
    public TwitterUser(Text userName,IntWritable totalMentionedCount) {
        this.userName = new Text(userName);
        this.totalMentionedCount = totalMentionedCount;
    }


    public IntWritable getTotalMentionedCount() {
        return totalMentionedCount;
    }

    public void setTotalMentionedCount(IntWritable totalMentionedCount) {
        int sum = this.getTotalMentionedCount().get() + totalMentionedCount.get();
        this.totalMentionedCount = new IntWritable(sum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            userName.write(dataOutput);
            totalMentionedCount.write(dataOutput);

        }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userName.readFields (dataInput);
        totalMentionedCount.readFields (dataInput);
    }

    @Override
    public String toString() {
        return "UserName: " + userName +" -> " + " Total number of Mentions :" + totalMentionedCount;
    }
}
