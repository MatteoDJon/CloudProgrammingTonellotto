package it.unipi.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.util.Random;

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

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, WritableWrapper> {

        private static final Random rng = new Random(31);

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            try {
                Point point = Point.parseString(value.toString());
                WritableWrapper wrapper = new WritableWrapper(point);

                int randomInt = rng.nextInt(20);
                if (randomInt == 7)
                    context.write(new IntWritable(randomInt), wrapper);
            } catch (ParseException e) {
                // ignore malformed points
            }
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable, Iterable<WritableWrapper>, IntWritable, WritableWrapper> {

        public void reduce(IntWritable key, Iterable<WritableWrapper> values, Context context)
                throws IOException, InterruptedException {

            Point point = null, tot = null;
            int count = 0;

            for (WritableWrapper wrapper : values) {
                point = wrapper.getPoint();

                if (tot == null)
                    tot = point;
                else
                    tot.sum(point);
                
                count++;
            }

            tot.divide(count);

            WritableWrapper result = new WritableWrapper(tot);
            context.write(key, result);
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

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
