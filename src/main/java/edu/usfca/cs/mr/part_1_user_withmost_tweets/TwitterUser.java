package edu.usfca.cs.mr.part_1_user_withmost_tweets;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TwitterUser implements Writable {
    private Text userName;
    private List<Text> timestamp;
    private List<Text> tweets;
    private IntWritable totalCount;

    public TwitterUser() {
        this.userName = new Text();
        this.timestamp = new ArrayList<Text>();
        this.tweets = new ArrayList<Text>();
        this.totalCount = new IntWritable(0);
    }
    public TwitterUser(Text userName, List<Text> timestamp, List<Text> tweets, IntWritable totalCount) {
        this.userName = new Text(userName);
        this.timestamp = timestamp;
        this.tweets = tweets;
        this.totalCount = totalCount;
    }
    public Text getUserName() {
        return userName;
    }

    public void setUserName(Text userName) {
        this.userName = userName;
    }

    public List<Text> getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(List<Text> timestamp) {
        this.timestamp.addAll(timestamp);
    }

    public List<Text> getTweets() {
        return tweets;
    }

    public void setTweets(List<Text> tweets) {
        this.tweets.addAll(tweets);
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
            userName.write(dataOutput);
            totalCount.write(dataOutput);
            dataOutput.writeInt(tweets.size());  //write size of list
            for(int index=0;index<tweets.size();index++){
            tweets.get(index).write(dataOutput); //write all the value of list
        }
        dataOutput.writeInt(timestamp.size());  //write size of list
        for(int index=0;index<timestamp.size();index++){
            timestamp.get(index).write(dataOutput); //write all the value of list
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userName.readFields (dataInput);
        totalCount.readFields (dataInput);
        int size1 = dataInput.readInt(); //read size of list
        tweets = new ArrayList<Text>(size1);
        for(int index=0;index<size1;index++){ //read all the values of list
            Text text = new Text();
            text.readFields(dataInput);
            tweets.add(text);
        }
       int size2 = dataInput.readInt(); //read size of list
        timestamp = new ArrayList<Text>(size2);
        for(int index=0;index<size2;index++){ //read all the values of list
            Text text1 = new Text();
            text1.readFields(dataInput);
            timestamp.add(text1);
        }
    }

    @Override
    public String toString() {
        return "UserName :" + "--> " + userName + " Total Tweet count " + totalCount + "\n" + "List of TimeStamps" +timestamp.toString() + "\n" + "List of Tweets" + tweets.toString() + "\n";
    }
}
