package edu.usfca.cs.mr.part_3_sentiment_analysis;

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
    private List<Text> tweets;
    private IntWritable sentimentCount;

    public TwitterUser() {
        this.userName = new Text();
        this.tweets = new ArrayList<Text>();
        this.sentimentCount = new IntWritable(0);
    }
    public TwitterUser(Text userName, List<Text> tweets, IntWritable sentimentCount) {
        this.userName = new Text(userName);
        this.tweets = tweets;
        this.sentimentCount = sentimentCount;
    }
    public Text getUserName() {
        return userName;
    }

    public void setUserName(Text userName) {
        this.userName = userName;
    }


    public List<Text> getTweets() {
        return tweets;
    }

    public void setTweets(List<Text> tweets) {
        this.tweets.addAll(tweets);
    }

    public IntWritable getSentimentCount() {
        return sentimentCount;
    }

    public void setSentimentCount(IntWritable totalCount) {
        int sum = this.getSentimentCount().get() + totalCount.get();
        this.sentimentCount = new IntWritable(sum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            userName.write(dataOutput);
            sentimentCount.write(dataOutput);
            dataOutput.writeInt(tweets.size());  //write size of list
            for(int index=0;index<tweets.size();index++){
            tweets.get(index).write(dataOutput); //write all the value of list
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userName.readFields (dataInput);
        sentimentCount.readFields (dataInput);
        int size1 = dataInput.readInt(); //read size of list
        tweets = new ArrayList<Text>(size1);
        for(int index=0;index<size1;index++){ //read all the values of list
            Text text = new Text();
            text.readFields(dataInput);
            tweets.add(text);
        }
    }

    @Override
    public String toString() {
        return ":" + " Total Sentiment count " + sentimentCount + "\n" + "List of Tweets" + tweets.toString() + "\n";
    }
}
