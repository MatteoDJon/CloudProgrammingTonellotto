package it.unipi.hadoop;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class UniformSampling {

    public static class UniformMapper extends Mapper<Object, Text, IntWritable, Text> {

        private static final IntWritable keyInt = new IntWritable();

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            int nStreams = context.getConfiguration().getInt("uniformSampling.nStreams", 1);
            for (int i = 0; i < nStreams; ++i){
                keyInt.set(i);
                context.write(keyInt, value);
            }
        }
    }

    public static class UniformReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        private static final Text p = new Text();

        public void reduce(final IntWritable key, final Iterable<Text> value, final Context context)
                throws IOException, InterruptedException {
            String str = null;
            long k = 0;
            for (Text t : value) {
                ++k;
                double random = ThreadLocalRandom.current().nextDouble(0, 1);
                if (random < ((double) 1 / k)){
                    str = t.toString(); //returns a new string
                }
            }
            p.set(str);
            context.write(NullWritable.get(), p);
        }
    }

    static Job createUniformSamplingJob(Configuration conf, Path inputFile, Path outputDir, int nStreams) throws IOException {
        conf.setInt("uniformSampling.nStreams", nStreams);
        Job job = Job.getInstance(conf, "uniformsampling");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(UniformSampling.class);

        job.setMapperClass(UniformSampling.UniformMapper.class);
        job.setReducerClass(UniformSampling.UniformReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(nStreams);
        return job;
    }
}