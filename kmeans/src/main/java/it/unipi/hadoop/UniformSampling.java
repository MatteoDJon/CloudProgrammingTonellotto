package it.unipi.hadoop;

import java.util.concurrent.ThreadLocalRandom;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UniformSampling {

    public static class UniformMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable one = new IntWritable(1);

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            /*
             * IntWritable v = new IntWritable(); int numeroVolte =
             * context.getConfiguration().getInt("k", 1); for (int i = 1; i <= numeroVolte;
             * ++i){ v.set(i); //oppure setInt context.write(v, value); }
             */
            context.write(one, value);
        }

    }

    public static class UniformReducer extends Reducer<IntWritable, Text, NullWritable, Point> {

        public void reduce(final IntWritable key, final Iterable<Text> value, final Context context)
                throws IOException, InterruptedException {
            Point p = null;
            long k = 0;
            for (Text t : value) {
                try {
                    Point temp = Point.parseString(t.toString());
                    ++k;
                    double random = ThreadLocalRandom.current().nextDouble(0, 1);
                    if (random < ((double) 1 / k))
                        p = temp;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            context.write(NullWritable.get(), p);
        }
    }
}