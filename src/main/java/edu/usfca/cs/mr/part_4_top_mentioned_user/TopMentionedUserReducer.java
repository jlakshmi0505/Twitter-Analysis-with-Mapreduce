package edu.usfca.cs.mr.part_4_top_mentioned_user;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TopMentionedUserReducer extends Reducer<Text, TopUserMentionedEachYear, Text, TwitterUser> {
    private Map<Text,ConcurrentSkipListSet<TwitterUser>> tagMap = new HashMap<>();
   // private final static Logger log = Logger.getLogger("TopMentionedUserReducer.class");
    ConcurrentSkipListSet<TwitterUser> map ;

    @Override
    protected void setup(Context context) {
        map = new ConcurrentSkipListSet<>();
        tagMap = new HashMap<>();
    }

    @Override
    protected void reduce(
            Text key, Iterable<TopUserMentionedEachYear> values, Context context) throws IOException, InterruptedException {
        ConcurrentSkipListSet<TwitterUser> userConcurrentSkipListSet = new ConcurrentSkipListSet<TwitterUser>(new UserMentionCountComparator());

        if (tagMap != null && tagMap.containsKey(key)) {
            userConcurrentSkipListSet.addAll(tagMap.get(key));
        }
        for (TopUserMentionedEachYear val : values) {
            for (TwitterUser twitterUser : val.getUserList()) {
                if (userConcurrentSkipListSet.contains(twitterUser)) {
                    updateUserMentionedCount(userConcurrentSkipListSet, twitterUser);
                } else {
                    userConcurrentSkipListSet.add(twitterUser);
                    removeLeastMentionCountUser(userConcurrentSkipListSet);
                }
            }

        }
        tagMap.put(new Text(key), userConcurrentSkipListSet);

    }

// remove the lowest mentioned count user
    private void removeLeastMentionCountUser(ConcurrentSkipListSet<TwitterUser> userConcurrentSkipListSet) {
        if (userConcurrentSkipListSet.size() > 5) {
            userConcurrentSkipListSet.pollFirst();
        }
    }

    private void updateUserMentionedCount(ConcurrentSkipListSet<TwitterUser> userConcurrentSkipListSet, TwitterUser twitterUser) {
        for(TwitterUser t : userConcurrentSkipListSet){
            if(t.getUserName().equals(twitterUser.getUserName())){
                TwitterUser user = new TwitterUser(twitterUser.getUserName(),twitterUser.getTotalMentionedCount());
                user.setTotalMentionedCount(t.getTotalMentionedCount());
                userConcurrentSkipListSet.remove(twitterUser);
                userConcurrentSkipListSet.add(user);
                removeLeastMentionCountUser(userConcurrentSkipListSet);
            }
        }
    }

    @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text key : tagMap.keySet()) {
                for(TwitterUser twitterUser : tagMap.get(key)){
                    context.write(new Text(key), twitterUser);
                }
            }
        }
    }

