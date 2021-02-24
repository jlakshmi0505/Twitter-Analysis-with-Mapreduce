package edu.usfca.cs.mr.part_2_top_hashtag;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HashTagByWeek implements Writable{
    private List<HashTag> hashTagList;

    public Text getWeekNum() {
        return weekNum;
    }

    public void setWeekNum(Text weekNum) {
        this.weekNum = weekNum;
    }

    private Text weekNum;

    public HashTagByWeek() {
        this.hashTagList = new ArrayList<>();
        this.weekNum = new Text();
    }
    public HashTagByWeek(List<HashTag> hashTagList, Text weekNum) {
        this.hashTagList = hashTagList;
        this.weekNum = weekNum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            weekNum.write(dataOutput);
            dataOutput.writeInt(hashTagList.size());
            for(int index=0;index<hashTagList.size();index++) {
               hashTagList.get(index).write(dataOutput);
            }
        }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        weekNum.readFields (dataInput);
        int size1 = dataInput.readInt();
        hashTagList = new ArrayList<>(size1);
        for(int index=0;index<size1;index++){
            HashTag hashTag = new HashTag();
            hashTag.readFields(dataInput);
            hashTagList.add(hashTag);
        }
    }

    @Override
    public String toString() {
        return "WeekNum :" + weekNum + " \n" + hashTagList.toString();
    }

    public List<HashTag> getHashTagList() {
        return hashTagList;
    }


}
