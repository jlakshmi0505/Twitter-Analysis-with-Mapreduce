package edu.usfca.cs.mr.part_4_top_mentioned_user;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopUserMentionedEachYear implements Writable{
    private List<TwitterUser> userList;
    private Text year;

    public TopUserMentionedEachYear() {
        this.userList = new ArrayList<>();
        this.year = new Text();
    }
    public TopUserMentionedEachYear(List<TwitterUser> userList, Text year) {
        this.userList = userList;
        this.year = year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            year.write(dataOutput);
            dataOutput.writeInt(userList.size());
            for(int index=0;index<userList.size();index++) {
               userList.get(index).write(dataOutput);
            }
        }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields (dataInput);
        int size1 = dataInput.readInt();
        userList = new ArrayList<TwitterUser>(size1);
        for(int index=0;index<size1;index++){
            TwitterUser twitterUser = new TwitterUser();
            twitterUser.readFields(dataInput);
            userList.add(twitterUser);
        }
    }

    @Override
    public String toString() {
        return year + " \n" + userList.toString();
    }

    public List<TwitterUser> getUserList() {
        return userList;
    }

}
