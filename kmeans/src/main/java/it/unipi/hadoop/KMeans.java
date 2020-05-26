package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

    private static final String cacheName = "centroids.txt";

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, WritableWrapper> {

        private int d;
        private int k;
        private List<Point> centroids;
        private IntWritable outputKey = new IntWritable(); // reuse
        private WritableWrapper outputValue = new WritableWrapper(); // reuse

        public void setup(Context context) throws IOException, InterruptedException {
            this.d = context.getConfiguration().getInt("kmeans.d", 7);
            this.k = context.getConfiguration().getInt("kmeans.k", 13);

            centroids = new ArrayList<>(k);

            Path cache = new Path(cacheName);
            FileSystem fs = cache.getFileSystem(context.getConfiguration());

            // read centroids from cache
            try (FSDataInputStream stream = fs.open(cache);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

                // read all lines from cache, use as centroids
                String line = null;
                while ((line = reader.readLine()) != null)
                    centroids.add(Point.parseString(line));

            } catch (IOException | ParseException e) {
                // TODO handle exception
            }

            // TODO check if centroids are not k

        }

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            try {
                Point point = Point.parseString(value.toString());

                // ignore, i.e. do not emit, points with dimension different from d
                if (point.getDimension() != d)
                    return;

                int nearestClusterId = -1;
                double minDistance = -1d;

                // find the nearest centroid
                int id = 0;
                for (Point centroid : centroids) {
                    double distance = point.computeDistance(centroid);
                    if (minDistance == -1 || distance < minDistance) {
                        minDistance = distance;
                        nearestClusterId = id;
                    }
                    id++;
                }

                // emit <clusterId, point>
                outputKey.set(nearestClusterId);
                outputValue.setPoint(point);
                context.write(outputKey, outputValue);

            } catch (ParseException e) {
                // ignore malformed points
                // TODO print something
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

            int count = 0;
            Point mean = new Point(d);

            for (WritableWrapper wrapper : values) {
                Point p = wrapper.getPoint();

                mean.sum(p);
                count++;
            }

            // compute mean point
            mean.divide(count);

            // emit <clusterId, (mean point, count)>
            outputKey = key;
            outputValue.setPoint(mean).setCount(count);
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        int d = 6;
        int k = 3;
        Configuration conf = new Configuration();
        conf.setInt("kmeans.d", d);
        conf.setInt("kmeans.k", k);

        Job job = Job.getInstance(conf, "kmeans");

        Path inputFile = new Path("data.txt");
        Path cacheFile = new Path("centroids.txt");
        Path outputDir = new Path("output");
        Path outputFile = new Path("output/part-r-00000");

        FileSystem fs = inputFile.getFileSystem(conf);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(WritableWrapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WritableWrapper.class);

        // select initial centroids and write them to file
        List<Point> centroids = getRandomCentroids(k, fs, inputFile);
        updateCache(centroids, fs, cacheFile);

        boolean success = true;
        int iteration = 1;
        List<Point> oldCentroids = centroids;
        List<Point> newCentroids = new ArrayList<>(k);
        while (success && iteration <= 1) {
            success = job.waitForCompletion(true);

            // readCentroidsFromOutput(newCentroids, fs, outputFile);

            System.out.println("Old centroids:");
            for (Point point : oldCentroids)
                System.out.println("\t" + point);

            System.out.println("New centroids:");
            for (Point point : newCentroids)
                System.out.println("\t" + point);

            iteration++;
        }

        System.exit(success ? 0 : 1);
    }

    private static List<Point> getRandomCentroids(int k, FileSystem fs, Path file) {

        List<Point> centroids = new ArrayList<>(k);

        // TODO improve random selection of centroids

        try (FSDataInputStream stream = fs.open(file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

            // read k lines, parse, use as centroids
            for (int i = 0; i < k; i++)
                centroids.add(Point.parseString(reader.readLine()));

        } catch (IOException | ParseException e) {
            // TODO handle exception
        }

        return centroids;
    }

    private static void readCentroidsFromOutput(List<Point> list, FileSystem fs, Path file) {

        list.clear();

        IntWritable id = new IntWritable();
        WritableWrapper wrapper = new WritableWrapper();

        try (FSDataInputStream fileStream = fs.open(file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream))) {
            
            String line = null;
            while ((line = reader.readLine()) != null) {
                InputStream lineStream = new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));
                DataInputStream in = new DataInputStream(lineStream);
                id.readFields(in);
                wrapper.readFields(in);

                list.add(wrapper.getPoint());
            }

        } catch (IOException e) {
            // TODO handle exception
            System.err.println("IOException");
        }

        // TODO maybe check if centroids are not k
    }

    private static void updateCache(List<Point> centroids, FileSystem fs, Path cache) {

        try (FSDataOutputStream stream = fs.create(cache, true);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream))) {

            // write each centroid in a line of the cache file
            for (Point centroid : centroids) {
                writer.write(centroid.toString());
                writer.newLine();
            }

        } catch (IOException e) {
            // TODO handle exception
        }
    }

}
