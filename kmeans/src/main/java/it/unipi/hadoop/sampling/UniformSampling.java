package it.unipi.hadoop.sampling;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniformSampling {

    public static class UniformMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        private int k;
        private PriorityQueue<PriorityLinePair> queue;
        private int size = 0;

        private static DoubleWritable wPriority = new DoubleWritable();
        private static Text wLine = new Text();

        public void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("uniformsampling.k", 13);
            queue = new PriorityQueue<>(k, new PriorityLinePairComparator());
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            double priority = ThreadLocalRandom.current().nextDouble(0, 1);

            // insert the first k points
            if (size < k) {
                queue.add(new PriorityLinePair(priority, value.toString()));
                size++;
            } else {
                // insert only if priority is in the k smallest priorities
                double maxPriority = queue.peek().priority;
                if (priority < maxPriority) {
                    queue.poll();
                    queue.offer(new PriorityLinePair(priority, value.toString()));
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            // emit <priority, line> for each of the k elements in the queue
            for (PriorityLinePair pair : queue) {
                wPriority.set(pair.priority);
                wLine.set(pair.line);
                
                context.write(wPriority, wLine);
            }
        }

    }

    public static class UniformReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

        private int k;
        private int emitted = 0;

        public void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("uniformsampling.k", 13);
        }

        public void reduce(DoubleWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            
            if (emitted == k)
                return;
            
            // emit just the first k lines you see
            for (Text line : value) {
                context.write(NullWritable.get(), line);
                emitted++;

                if (emitted == k)
                    return;
            }
        }
    }

    public static Job createJob(Configuration conf, Path inputFile, Path outputDir) throws IOException {
        Job job = Job.getInstance(conf, "uniformsampling");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(UniformSampling.class);

        job.setMapperClass(UniformMapper.class);
        job.setReducerClass(UniformReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        return job;
    }
}