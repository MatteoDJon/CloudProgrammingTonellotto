package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

    private static final String cacheName = "centroids.txt";
    private static final String outputFileName = "part-r-00000";

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, WritableWrapper> {

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable, Iterable<WritableWrapper>, IntWritable, WritableWrapper> {

        public void reduce(IntWritable key, Iterable<WritableWrapper> values, Context context)
                throws IOException, InterruptedException {
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "kmeans");

        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WritableWrapper.class);

        Path inputFile = new Path("data.txt");
        Path outputDir = new Path("output");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);
    }

}
