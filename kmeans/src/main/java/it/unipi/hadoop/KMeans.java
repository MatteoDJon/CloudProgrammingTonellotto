package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {

    private static String outputName = "centroids.txt";
    private static String inputName = "data.txt";
    private static String KMeansDirName = "KMeansTemporary";
    //part-00000 or part-r-00000 are ok
    private static final Pattern pattern = Pattern.compile("part-(r-)?[\\d]{5}"); 

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
                // TODO handle exception
            }

            // check if centroids are not k
            if (centroids.size() != this.k) {
                System.err.println("Wrong centroids number");
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
                double minDistance = -1d;

                // find the nearest centroid
                int id = 0;
                for (Point centroid : centroids) {
                    double distance = point.computeSquaredDistance(centroid);
                    if (minDistance == -1 || distance < minDistance) {
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

    public static void main(String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        /*  
            otherArgs[0] = d;
            otherArgs[1] = k;
            otherArgs[2] = inputName
            otherArgs[3] = outputName
            KMeansSamplingDirName può essere construito concatenando KMeansDir con "Sampling"
        */
        int d = 6;
        int k = 3;
        /*
        if (otherArgs.length < 2){
            System.out.println("KMeans [d k inputName KMeansDirName outputName]");
        }
        */

        switch(otherArgs.length){
            default:
            case 4:
                outputName = otherArgs[3];
            case 3:
                inputName = otherArgs[2];
            case 2:
                k = Integer.parseInt(otherArgs[1]);
            case 1:
                d = Integer.parseInt(otherArgs[0]);
                break;
            case 0:
                System.out.println("Caso default");
        }
        System.out.println("Uso parametri d = " + Integer.toString(d) + ", k = " + Integer.toString(k));
        System.out.println("Nome file input = " + inputName + ", file output = " + outputName);
        Configuration conf = new Configuration();
        conf.setInt("kmeans.d", d);
        conf.setInt("kmeans.k", k);

        Path inputFile = new Path(inputName);
        Path outputFile = new Path(outputName);
        Path KMeansDir = new Path(KMeansDirName);
        Path KMeansSamplingDir = new Path(KMeansDirName + "UniformSampling");
        FileSystem fs = FileSystem.get(conf);


        // select initial centroids and write them to file
        List<Point> centroids = getRandomCentroids(conf, k, fs, inputFile, KMeansSamplingDir);
        updateCache(centroids, fs, outputFile);
        boolean success = true;
        int iteration = 1;
        List<Point> currentCentroids = centroids;
        List<Point> newCentroids = new ArrayList<>(k);

        double convergeDist = 0.00001;
        double tempDistance = convergeDist + 0.1;

        // delete the output directory if exists
        if (fs.exists(KMeansDir))
            fs.delete(KMeansDir, true);
        
        while (success && tempDistance > convergeDist && iteration <= 10) {


            // new job to compute new centroids
            Job kmeansJob = createKMeansJob(conf, inputFile, KMeansDir);
            success = kmeansJob.waitForCompletion(true);
            // add new centroids to list
            readCentroidsFromOutput(newCentroids, fs, KMeansDir);

            System.out.println("New centroids " + iteration + ":");
            for (Point point : newCentroids)
                System.out.println("\t" + point);

            // write new centroids to file read from mapper
            updateCache(newCentroids, fs, outputFile);

            // sum of all the relative errors between old centroids and new centroids
            
            tempDistance = compareCentroids(currentCentroids, newCentroids);
            
            // before next iteration update old centroids list
            List<Point> tempList = currentCentroids;
            currentCentroids = newCentroids;
            newCentroids = tempList;

            iteration++;
            System.out.println("tempDistance = " + Double.toString(tempDistance)); // DEBUG
            // delete the output directory if exists
            if (fs.exists(KMeansDir))
                fs.delete(KMeansDir, true);
        }
        System.exit(success ? 0 : 1);
    }


    private static double compareCentroids(List<Point> oldCentroids, List<Point> newCentroids) {
        // precondizione: i centroidi sono, rispetto alla singola posizione,
        // la versione precedente e la versione nuova

        double tempDistance = 0.0;

        for (int i = 0; i < oldCentroids.size(); i++) {

            Point oldCentroid = oldCentroids.get(i);
            Point newCentroid = newCentroids.get(i);

            double dist = oldCentroid.computeSquaredDistance(newCentroid);

            double norm1 = oldCentroid.computeSquaredNorm();
            double norm2 = newCentroid.computeSquaredNorm();
            double minNorm = Double.min(norm1, norm2);

            tempDistance += (dist / minNorm);
        }

        return tempDistance;
    }

    private static Job createKMeansJob(Configuration conf, Path inputFile, Path outputDir) throws IOException {
        Job job = Job.getInstance(conf, "kmeans");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        // job.setCombinerClass(KMeansReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(WritableWrapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WritableWrapper.class);

        return job;
    }

    private static Job createUniformSamplingJob(Configuration conf, Path inputFile, Path outputDir) throws IOException {
        Job job = Job.getInstance(conf, "uniformsampling");

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setJarByClass(UniformSampling.class);

        job.setMapperClass(UniformSampling.UniformMapper.class);
        job.setReducerClass(UniformSampling.UniformReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        return job;
    }

    private static List<Path> getOutputFiles(FileSystem fs, Path outputDir) throws Exception {
        List<Path> listPaths = new ArrayList<>();
        Matcher matcher;
        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(outputDir, false);
        while (rit.hasNext()){
            Path outputFile = rit.next().getPath();
            matcher = pattern.matcher(outputFile.toString());
            if (matcher.find()){
                listPaths.add(outputFile);
            }
        }
        return listPaths;

    }
    private static List<Point> getRandomCentroids(Configuration conf, int k, FileSystem fs, Path inputFile, Path outputDir) throws Exception {

        List<Point> centroids = new ArrayList<>(k);
        int i = 0;
        //simple control, just in case
        if (fs.exists(outputDir)){
            fs.delete(outputDir, true);
        }
        while (i < k){
            Job job = createUniformSamplingJob(conf, inputFile, outputDir);
            job.waitForCompletion(true);
            //prelevare i punti e aggiungerli alla lista
            List<Path> outputFiles = getOutputFiles(fs, outputDir);
            
            for (Path outputFile : outputFiles){
                try (FSDataInputStream stream = fs.open(outputFile); 
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream)))
                {
                    Point p = Point.parseString(reader.readLine());
                    boolean unique = true;
                    for (Point c : centroids) {
                        if (c.equals(p)) {
                            unique = false;
                            break;
                        }
                    }
                    if (unique) {
                        centroids.add(p);
                            ++i;
                        }
                    }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
            if (fs.exists(outputDir)){
                fs.delete(outputDir, true);
            }
        }

        return centroids;
    }

    private static void readCentroidsFromOutput(List<Point> list, FileSystem fs, Path outputDir) throws Exception{

        list.clear();

        WritableWrapper wrapper;
        List<Path> listOutputFiles = getOutputFiles(fs, outputDir);
        for (Path file: listOutputFiles){
            try (FSDataInputStream fileStream = fs.open(file);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream))) {

                String line = null;
                while ((line = reader.readLine()) != null) {

                    String[] split = line.split("\t"); // split key and value
                    String value = split[1]; // get wrapper string

                    wrapper = WritableWrapper.parseString(value);

                    // divide to compute mean point
                    int count = wrapper.getCount();
                    Point centroid = wrapper.getPoint();
                    centroid.divide(count);

                    list.add(centroid);
                }

            } catch (IOException | ParseException e) {
                // TODO handle exception
                System.err.println("Exception: " + e.getMessage());
            }
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