package edu.usfca.cs.mr.part_2_top_hashtag;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TopHashTagReducer extends Reducer<Text, HashTagByWeek, Text, HashTag> {
    private Map<Text,ConcurrentSkipListSet<HashTag>> tagMap;
    private final static Logger log = Logger.getLogger("TopHashTagReducer.class");
    ConcurrentSkipListSet<HashTag> map ;

    @Override
    protected void setup(Context context) {
        map = new ConcurrentSkipListSet<>();
        tagMap = new HashMap<>();
    }

    @Override
    protected void reduce(
            Text key, Iterable<HashTagByWeek> values, Context context){
        ConcurrentSkipListSet<HashTag> listSet = new ConcurrentSkipListSet<HashTag>(new HashTagCountComparator());

        if (tagMap != null && tagMap.containsKey(key)) {
            listSet.addAll(tagMap.get(key));
        }

        for (HashTagByWeek val : values) {
            for (HashTag hashTag : val.getHashTagList()) {
                if (listSet.contains(hashTag)) {
                    updateHashTagCount(listSet, hashTag);
                } else {
                    listSet.add(hashTag);
                    if (listSet.size() > 5) {
                        listSet.pollFirst();
                    }
                }
            }
        }
        tagMap.put(new Text(key), listSet);

    }

    private void updateHashTagCount(ConcurrentSkipListSet<HashTag> concurrentSkipListSet, HashTag hashTag) {
        for(HashTag t : concurrentSkipListSet){
            if(t.getHashTagName().equals(hashTag.getHashTagName())){
                HashTag hashTag1 = new HashTag(hashTag.getHashTagName(),hashTag.getTotalCount());
                hashTag1.setTotalCount(t.getTotalCount());
                concurrentSkipListSet.remove(hashTag);
                concurrentSkipListSet.add(hashTag1);
                if (concurrentSkipListSet.size() > 5){
                    concurrentSkipListSet.pollFirst();
                }
            }
        }
    }

    @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text key : tagMap.keySet()) {
                for(HashTag hashTag : tagMap.get(key)){
                    context.write(new Text(key), hashTag);
                }
            }
        }
    }

