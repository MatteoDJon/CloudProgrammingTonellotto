package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

        private int d;
        private int k;
        private List<Point> centroids;

        private static IntWritable outputKey = new IntWritable(); // reuse
        private static WritableWrapper outputValue = new WritableWrapper(); // reuse

        public void setup(Context context) throws IOException, InterruptedException {
            this.d = context.getConfiguration().getInt("kmeans.d", 7);
            this.k = context.getConfiguration().getInt("kmeans.k", 13);
            String outputName = context.getConfiguration().get("kmeans.output", "centroids.txt");

            centroids = new ArrayList<>(k);

            Path cache = new Path(outputName);
            FileSystem fs = cache.getFileSystem(context.getConfiguration());

            // read centroids from cache
            try (FSDataInputStream stream = fs.open(cache);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

                // read all lines from cache, use as centroids
                String line = null;
                while ((line = reader.readLine()) != null) {
                    Point newPoint = Point.parseString(line);

                    if (newPoint.getDimension() != this.d) {
                        System.err.println("Wrong centroids dimension");
                        System.exit(1);
                    }
                    centroids.add(newPoint);
                }
            } catch (IOException | ParseException e) {
                System.err.println("Could not read centroids correctly");
                System.exit(1);
            }

            // check if centroids are not k
            if (centroids.size() != this.k) {
                System.err.println("Wrong centroids number");
		for (Point p: centroids)
			System.err.println(p.toString());
		System.err.println("These were the points read");
                System.exit(1);
            }
        }

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            try {
                Point point = Point.parseString(value.toString());

                // ignore, i.e. do not emit, points with dimension different from d
                if (point.getDimension() != d)
                    return;

                int nearestClusterId = -1;
                double minDistance = Double.POSITIVE_INFINITY;

                // find the nearest centroid
                int id = 0;
                for (Point centroid : centroids) {
                    double distance = point.computeSquaredDistance(centroid);
                    if (distance < minDistance) {
                        minDistance = distance;
                        nearestClusterId = id;
                    }
                    id++;
                }

                // emit <clusterId, point>
                outputKey.set(nearestClusterId);
                outputValue.setPoint(point).setCount(1);
                context.write(outputKey, outputValue);

            } catch (ParseException e) {
                // ignore malformed points
                System.err.println("Could not parse '" + value.toString() + "'");
            }
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, WritableWrapper, IntWritable, WritableWrapper> {

        private IntWritable outputKey = new IntWritable(); // reuse
        private WritableWrapper outputValue = new WritableWrapper(); // reuse
        private int d;

        public void setup(Context context) throws IOException, InterruptedException {
            this.d = context.getConfiguration().getInt("kmeans.d", 7);
        }

        public void reduce(IntWritable key, Iterable<WritableWrapper> values, Context context)
                throws IOException, InterruptedException {

            int totCount = 0;
            Point sum = new Point(d); // init with 0, 0, ..., 0

            for (WritableWrapper wrapper : values) {
                Point p = wrapper.getPoint();

                sum.sum(p);
                totCount += wrapper.getCount();
            }

            // compute mean point
            // sum.divide(totCount);

            // emit <clusterId, (sum point, count)>
            outputKey = key;
            outputValue.setPoint(sum).setCount(totCount);
            context.write(outputKey, outputValue);
        }
    }

    static Job createKMeansJob(Configuration conf, Path inputFile, Path outputDir, Path outputFile, int d, int k) throws IOException {

        conf.setInt("kmeans.d", d);
        conf.setInt("kmeans.k", k);
        conf.set("kmeans.output", outputFile.toString());

        Job job = Job.getInstance(conf, "kmeans");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setCombinerClass(KMeansReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(WritableWrapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WritableWrapper.class);

        job.setNumReduceTasks(k);

        return job;
    }

}
