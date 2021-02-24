package edu.usfca.cs.mr.part_2_top_hashtag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class TopHashtagJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Top Hash tag Job");

            /* Current class */
            job.setJarByClass(TopHashtagJob.class);

            /* Mapper class */
            job.setMapperClass(TopHashTagMapper.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:
            //job.setCombinerClass(WordCountReducer.class);

            /* Reducer class */
            job.setReducerClass(TopHashTagReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HashTagByWeek.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(HashTag.class);

            /* Reduce tasks */
            job.setNumReduceTasks(1);

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
