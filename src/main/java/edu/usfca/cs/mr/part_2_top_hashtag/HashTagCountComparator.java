package edu.usfca.cs.mr.part_2_top_hashtag;

import java.util.Comparator;

public class HashTagCountComparator implements Comparator<HashTag> {
    @Override
    public int compare(HashTag o1, HashTag o2) {
        if(o1.getHashTagName().toString().equals(o2.getHashTagName().toString())){
            return 0;
        }
        if (o1.getTotalCount().get() >= o2.getTotalCount().get()) {
            return 1;
        }
        else{
            return -1;
        }
    }
}
