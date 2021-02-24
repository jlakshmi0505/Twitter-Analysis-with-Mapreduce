package edu.usfca.cs.mr.part_4_top_mentioned_user;

import java.util.Comparator;

public class UserMentionCountComparator implements Comparator<TwitterUser> {
    @Override
    public int compare(TwitterUser o1, TwitterUser o2) {
        if(o1.getUserName().toString().equals(o2.getUserName().toString())){
            return 0;
        }
        if (o1.getTotalMentionedCount().get() >= o2.getTotalMentionedCount().get()) {
            return 1;
        }
        else{
            return -1;
        }
    }
}
