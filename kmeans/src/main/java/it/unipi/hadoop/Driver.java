package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.sampling.UniformSampling;

public class Driver {

    private static final double CONVERGENCE_THRESHOLD = 1e-4;
    private static final int MAX_ITERATIONS = 100;

    private static String outputName = "centroids.txt";
    private static String inputName = "data.txt";
    private static final String KMeansDirName = "KMeansTemporary";
    private static final String SAMPLED_CENTROIDS_DIR = "UniformSampling";

    private static final Pattern outputFilesPattern = Pattern.compile("part-(r-)?[\\d]{5}");

    private static List<Path> getOutputFiles(FileSystem fs, Path outputDir)
            throws FileNotFoundException, IOException {

        List<Path> listPaths = new ArrayList<>();

        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(outputDir, false);

        while (rit.hasNext()) {
            Path outputFile = rit.next().getPath();
            Matcher matcher = outputFilesPattern.matcher(outputFile.toString());

            if (matcher.find())
                listPaths.add(outputFile);
        }

        return listPaths;
    }

    private static boolean executeUniformSamplingJob(Configuration conf, int k, Path inputFile, Path outputDir)
            throws Exception {

        conf.setInt("uniformsampling.k", k);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputDir))
            fs.delete(outputDir, true);
  
        Job job = UniformSampling.createJob(conf, inputFile, outputDir);

        return job.waitForCompletion(true);
    }

    private static List<Point> readSampledCentroids(int k, FileSystem fs) {

        List<Point> centroids = new ArrayList<>(k);

        Path centroidsFile = new Path(SAMPLED_CENTROIDS_DIR, "part-r-00000");

        try (FSDataInputStream stream = fs.open(centroidsFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
    
            String line;
            while ((line = reader.readLine()) != null) {
                Point p = Point.parseString(line);
                centroids.add(p);
            }

        } catch (IOException e) {
            System.err.println("Could not read sampled centroids");
            System.err.println(e.getMessage());
            return null;
        } catch (ParseException e) {
            System.err.println("Malformed point string");
            return null;
        }
        
        if (centroids.size() != k) {
            System.err.println("Number of read centroids is not k");
            return null;
        }

        if (new HashSet<Point>(centroids).size() != k) {
            System.err.println("Found some duplicate centroids");
            return null;
        }

        return centroids;
    }

    private static void updateCache(List<Point> centroids, FileSystem fs, Path cache) throws IOException {

        try (FSDataOutputStream stream = fs.create(cache, true);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream))) {

            // write each centroid in a line of the cache file
            for (Point centroid : centroids) {
                writer.write(centroid.toString());
                writer.newLine();
            }

        } catch (IOException e) {
            System.out.println("Could not update centroids cache '" + cache.toString() + "'");
            throw e;
        }
    }

    static void readCentroidsFromOutput(List<Point> list, FileSystem fs, Path outputDir) throws Exception {

        list.clear();

        WritableWrapper wrapper;
        List<Path> listOutputFiles = getOutputFiles(fs, outputDir);
        for (Path file : listOutputFiles) {
            try (FSDataInputStream fileStream = fs.open(file);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream))) {

                String line = null;
                while ((line = reader.readLine()) != null) {

                    String[] split = line.split("\t"); // split key and value
                    Integer position = Integer.parseInt(split[0]); // key in the list
                    String value = split[1]; // get wrapper string

                    wrapper = WritableWrapper.parseString(value);

                    // divide to compute mean point
                    int count = wrapper.getCount();
                    Point centroid = wrapper.getPoint();
                    centroid.divide(count);

                    // Don't know if I can trust the position of the files for the new centroids
                    // Important: I assume that a key starts from 0 (really important)
                    while (list.size() <= position)
                        list.add(null);
                    
                    list.set(position, centroid);
                }

            } catch (IOException | ParseException e) {
                System.err.println("Could not read centroids of last iteration");
                System.err.println("Exception: " + e.getMessage());
                System.exit(1);
            }
        }

    }

    static double compareCentroids(List<Point> oldCentroids, List<Point> newCentroids) {
        // precondizione: i centroidi sono, rispetto alla singola posizione,
        // la versione precedente e la versione nuova --> stessa dimensione e stesso
        // numero

        double maxDistance = 0.0;

        for (int i = 0; i < oldCentroids.size(); i++) {

            Point oldCentroid = oldCentroids.get(i);
            Point newCentroid = newCentroids.get(i);

            double distance = oldCentroid.computeSquaredDistance(newCentroid);
            maxDistance = Double.max(maxDistance, distance);
        }

        return maxDistance;
    }

    public static void main(String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        
        // List of arguments = [dimensionPoints numberCentroids pointsPath finalCentroidsPath];
        // otherArgs[0] = d;
        // otherArgs[1] = k;
        // otherArgs[2] = inputName;
        // otherArgs[3] = outputName;
        // otherArgs[4] = n;
        // KMeansSamplingDirName può essere construito concatenando KMeansDir con "Sampling"
         
        String n = "____";
        int d = 3; // default
        int k = 7; // default
        switch (otherArgs.length) {
            default:
            case 5:
                n = otherArgs[4];
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
        // System.out.println("Uso parametri d = " + Integer.toString(d) + ", k = " + Integer.toString(k));
        // System.out.println("Nome file input = " + inputName + ", file output = " + outputName);

        Configuration conf = new Configuration();
        Path inputFile = new Path(inputName);
        Path outputFile = new Path(outputName);
        Path kMeansDir = new Path(KMeansDirName);
        Path samplingDir = new Path(SAMPLED_CENTROIDS_DIR);
        FileSystem fs = FileSystem.get(conf);

        // cleanup everything there is as output
        if (fs.exists(outputFile))
            fs.delete(outputFile, true);

        long startTime = System.currentTimeMillis();
        
        // select randomly k initial centroids
        boolean success = executeUniformSamplingJob(conf, k, inputFile, samplingDir);
        List<Point> initialCentroids = readSampledCentroids(k, fs);

        long samplingEnd = System.currentTimeMillis();

        // write initial centroids file in hdfs
        updateCache(initialCentroids, fs, outputFile);

        int iteration = 0;
        List<Point> currentCentroids = initialCentroids;
        List<Point> newCentroids = new ArrayList<>(k);

        // delete the output directory if exists
        if (fs.exists(kMeansDir))
            fs.delete(kMeansDir, true);

        double tempDistance = Double.POSITIVE_INFINITY;
        long kMeansStart = System.currentTimeMillis();
        
        while (success && tempDistance > CONVERGENCE_THRESHOLD && iteration <= MAX_ITERATIONS) {
            iteration++;

            // new job to compute new centroids
            Job kmeansJob = KMeans.createKMeansJob(conf, inputFile, kMeansDir, outputFile, d, k);
            success = kmeansJob.waitForCompletion(true);

            // add new centroids to list
            readCentroidsFromOutput(newCentroids, fs, kMeansDir);

            // write new centroids to file so that mappers can read them
            updateCache(newCentroids, fs, outputFile);

            // sum of all the relative errors between old centroids and new centroids
            tempDistance = compareCentroids(currentCentroids, newCentroids);

            // before next iteration update old centroids list
            List<Point> tempList = currentCentroids;
            currentCentroids = newCentroids;
            newCentroids = tempList;
            
            // delete the output directory if exists
            if (fs.exists(kMeansDir))
                fs.delete(kMeansDir, true);
        }
        
        long endTime = System.currentTimeMillis();

        boolean converged = tempDistance <= CONVERGENCE_THRESHOLD;

        // System.out.println("Total execution time:    " + secondsBetween(startTime, endTime) + " s");
        // System.out.println("  uniform sampling:      " + secondsBetween(startTime, samplingEnd) + " s");
        // System.out.println("  k-means algorithm:     " + secondsBetween(kMeansStart, endTime) + " s");

        // System.out.println("Number of iterations:    " + iteration);
        // System.out.println("Centroids last movement: " + tempDistance);
        // System.out.println("Success && converged:    " + ((success && converged) ? "yes" : "no"));

        String metricsFile = String.format("hadoop_n=%s_d=%d_k=%d.csv", n, d, k);

        PerformanceMetrics metrics = new PerformanceMetrics();
        metrics.totalExecutionTime = secondsBetween(startTime, endTime);
        metrics.samplingExecutionTime = secondsBetween(startTime, samplingEnd);
        metrics.kMeansExecutionTime = secondsBetween(kMeansStart, endTime);
        metrics.iterations = iteration;
        metrics.centroidsLastMovement = tempDistance;
        metrics.successful = success && converged;
        metrics.saveToFile(metricsFile);

        System.exit(success ? 0 : 1);
    }

    private static double secondsBetween(long from, long to) {
        return (double) (to - from) / 1000;
    }

    private static class PerformanceMetrics {
        double totalExecutionTime;
        double samplingExecutionTime;
        double kMeansExecutionTime;
        int iterations;
        double centroidsLastMovement;
        boolean successful;

        void saveToFile(String file) {
            // formatter for float and double
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(2);
            df.setMinimumFractionDigits(2);
            df.setGroupingUsed(false);

            // write performance metrics in append
            try (FileWriter fw = new FileWriter(file, true);
                    BufferedWriter bw = new BufferedWriter(fw);
                    PrintWriter fout = new PrintWriter(bw)) {

                // csv
                String line = String.format("%s,%s,%s,%d,%.9f,%b",
                        df.format(totalExecutionTime),
                        df.format(samplingExecutionTime),
                        df.format(kMeansExecutionTime),
                        iterations,
                        centroidsLastMovement,
                        successful);
                fout.println(line);

            } catch (IOException e) {
                System.err.println("Could not write performance to file");
            }
        }
    }
}


