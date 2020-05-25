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

                int randomInt = rng.nextInt(100);
                if (randomInt == 5 || randomInt == 21)
                    context.write(new IntWritable(randomInt), new WritableWrapper(point));
            } catch (ParseException e) {
                // ignore malformed points
            }
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable, WritableWrapper, Text, Text> {

        public void reduce(IntWritable key, Iterable<WritableWrapper> values, Context context)
                throws IOException, InterruptedException {
            
            int count = 0;

            for (WritableWrapper wrapper : values)                
                count += wrapper.getOne();
            
            context.write(new Text(Integer.toString(key.get())), new Text(Integer.toString(count)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "kmeans");

        Path inputFile = new Path("data.txt");
        Path outputDir = new Path("output");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(WritableWrapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
