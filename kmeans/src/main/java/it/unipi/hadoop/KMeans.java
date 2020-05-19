package it.unipi.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class KMeans {
    private static final int K_INDEX = 0;
    private static final int INPUT_INDEX = 1;
    private static final int OUTPUT_INDEX = 2;

    private static final String cacheName = "centroids.txt";
    private static final String outputFileName = "/part-r-00000";

    public static class NewMapper extends Mapper<Object, Text, IntWritable, WritableWrapper> {
        // out: key - clusterId, value - < point, dimension, 1 >

        private int D = -1;
        private List<Point> centroids = new ArrayList<>();

        public void setup(Context context) {
            try {
                File file = new File(context.getCacheFiles()[0]);
                FileReader fr = new FileReader(file);

                BufferedReader br = new BufferedReader(fr);
                String line;

                while ((line = br.readLine()) != null)
                    centroids.add(new Point(line));
                br.close();

            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            int dim = centroids.get(0).getDimension();
            for (Point c : centroids)
                if (dim != c.getDimension())
                    return;

            D = dim;
        }

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            // controllo che tutti i centroidi abbiano la stessa dimensione
            if (D < 0)
                // TODO - gestione caso d'errore
                return;
            Point p = null;
            try {
                p = new Point(value.toString());
            } catch (Exception e) {
                e.printStackTrace();
                return; // check later if we have to exit or not
            }

            // calculate the distance between p and the centroids
            int id = 0;
            int clusterId = 0;
            double minDistance = -1;

            for (Point c : centroids) {
                double distance = p.computeDistance(c);
                if (minDistance == -1 || distance < minDistance) {
                    minDistance = distance;
                    clusterId = id;
                }
                id++;
            }

            context.write(new IntWritable(clusterId), new WritableWrapper(p, D));
            // key : clusterId, value : <point, centroids dimension, one>
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable, Iterable<WritableWrapper>, IntWritable, WritableWrapper> {
        // out : key - clusterId, values - <Point, centroids dimension, Denominator>

        public void reduce(IntWritable key, Iterable<WritableWrapper> values, Context context)
                throws IOException, InterruptedException {
            int d = -1;
            int id = 0;

            Point numerator = null; // costruttore di un punto di dim : d e valori : 0
            int denominator = 0;

            for (WritableWrapper val : values) {
                if (id == 0) {
                    // primo step
                    // inizializzazione point di tutti 0 di dimensione d = dimensione dei centroidi
                    d = val.getDimension();
                    numerator = new Point(d);
                }
                id++;
                try {
                    numerator.sum(val.getPoint());
                } catch (Exception e) {
                    System.out.println("WRN: Point dimension mismatch");
                }

                denominator += val.getOne();
            }
            context.write(key, new WritableWrapper(numerator, d, denominator));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // if (otherArgs.length < 3) {
        //    System.err.println("Usage: kmeans k: <k parameter> <path-file> <output file>");
        //    System.exit(2);
        // }

        // int K = Integer.parseInt(otherArgs[K_INDEX]);
        int K = 3;

        Job job = Job.getInstance(conf, "k means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(NewMapper.class);
        // job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WritableWrapper.class);

        // Path inputFile = new Path(otherArgs[INPUT_INDEX]);
        // Path outputDir = new Path(otherArgs[OUTPUT_INDEX]);
        // Path outputFile = new Path(otherArgs[OUTPUT_INDEX] + outputFileName);
        // Path cacheFile = new Path(cacheName);
        
        Path inputFile = new Path( "data.txt" );
        Path outputDir = new Path( "output" );
        Path outputFile = new Path( "output" + outputFileName);
        Path cacheFile = new Path(cacheName);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        // generare k indici di riga da selezionare a caso
        // aprire uno stream dal file
        // prendere le righe e considerarle centroidi
        // salvare i centroidi in un file da usare come cache
        Set<Long> indexesCentroids = new HashSet<>();
        Random rand = new Random();
        List<Point> centroids = new ArrayList<>(K);
        FileSystem fs = inputFile.getFileSystem(conf);

        //long numberOfLines = Files.lines(Paths.get(inputFile.toUri())).count();
        long numberOfLines = 0;
        try (FSDataInputStream inputStream = fs.open(inputFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine();
            while (line != null) {
                ++numberOfLines;
                line = reader.readLine();
            }
        } catch (Exception ex) {
            System.out.println("Problem with reading number of points in hadoop");
            System.exit(1);
        }

        while (indexesCentroids.size() < K) {
            long valueRandom = rand.nextLong();
            if (valueRandom < numberOfLines)
                indexesCentroids.add(valueRandom);
        }

        try (FSDataInputStream inputStream = fs.open(inputFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            numberOfLines = 0;
            String line = reader.readLine();
            while (line != null) {
                if (indexesCentroids.contains(numberOfLines))
                    centroids.add(new Point(line));
                ++numberOfLines;
                line = reader.readLine();
            }
        } catch (Exception ex) {
            System.out.println("Problem with reading file points in hadoop");
            System.exit(1);
        }

        // all centroids are taken from the list of points

        try (FSDataOutputStream outputStream = fs.create(cacheFile);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            for (Point line : centroids) {
                writer.write(line.toString(), 0, line.toString().length());
                writer.newLine();
            }
        } catch (Exception ex) {
            System.out.println("Problem with writing file centroids in hadoop");
            System.exit(1);
        }

        // add file cache
        job.addCacheFile(cacheFile.toUri());

        List<Point> newCentroids = new ArrayList<>(K);
        for (int i = 0; i < K; ++i) {
            newCentroids.add(null);
        }

        int iterations = 0;
        boolean stop = false;
        while (iterations < 10 && !stop) {
            job.waitForCompletion(true);
            // find centroid
            try (FSDataInputStream inputStream = fs.open(outputFile)) {
                // output: clusterid WritableWrapper
                // in questo loop recuperare i nuovi centroidi e poi conservarli in una lista
                // Dopo di ciò confrontare i vecchi centroidi con i nuovi centroidi: se sono
                // uguali, break dal loop while
                // Se non sono uguali, allora aggiornare i centroidi nel file cache
                IntWritable clusterId = new IntWritable();
                WritableWrapper wr = new WritableWrapper();
                while (inputStream.available() > 0) {
                    // get cluster ID and compute the rest to newCentroids

                    clusterId.readFields(inputStream);
                    wr.readFields(inputStream);

                    Point centroideNuovo = wr.getPoint(); // da popolare con il centroide
                    centroideNuovo.divide(wr.getOne());
                    newCentroids.set(clusterId.get(), centroideNuovo);
                }

            } catch (Exception ex) {
                System.out.println("Problem with reading file points in hadoop");
                System.exit(1);
            }
            // check conditions
            stop = true;
            for (int i = 0; i < K; i++) {
                if (!centroids.get(i).compare(newCentroids.get(i))) {
                    stop = false;
                    // update
                    centroids = newCentroids;
                    newCentroids = new ArrayList<>(K);
                    for (int j = 0; j < K; ++j) {
                        newCentroids.add(null);
                    }
                    break;
                }
            }

            iterations++;
        }
        for (int i = 0; i < K; ++i) {
            centroids.get(i).print();
        }
    }

}
