package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class KMeans {
    private static final int K_INDEX = 0;
    private static final int INPUT_INDEX = 1;
    private static final int OUTPUT_INDEX = 2;

    private static final String cacheName = "centroids.txt";
    private static final String outputFileName = "/part-r-00000";

    public static class NewMapper extends Mapper<Object, Text, IntWritable, WritableWrapper> {
        // out: key - clusterId, value - < point, dimension, 1 >
        private Logger logger = Logger.getLogger(NewMapper.class);
        private int D = -1;
        private List<Point> centroids = new ArrayList<>();

        public void setup(Context context) {

            Path cacheFile;
            FileSystem fs;
            try {
                cacheFile = new Path(cacheName);
                fs = cacheFile.getFileSystem(context.getConfiguration());
            } catch (Exception e) {
                logger.error(e.getMessage());
                return;
            }

            try (FSDataInputStream inputStream = fs.open(cacheFile);
                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                logger.debug(context.getCacheFiles()[0].toString()); // DEBUG
                // File file = new File(context.getCacheFiles()[0]);
                // FileReader fr = new FileReader(file);

                // BufferedReader br = new BufferedReader(fr);
                String line;

                while ((line = br.readLine()) != null)
                    centroids.add(Point.parseString(line));

            } catch (Exception e) {
                logger.debug(e.getMessage());
                return;
            }

            int dim = centroids.get(0).getDimension();
            for (Point c : centroids)
                if (dim != c.getDimension()) {
                    logger.debug("Punto con dimensione -" + Integer.toString(dim) + "- non valida"); // DEBUG
                    return;
                }

            D = dim;
        }

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            // controllo che tutti i centroidi abbiano la stessa dimensione
            if (D < 0) {
                // TODO - gestione caso d'errore
                logger.debug("D < 0 nella map()");

                return;
            }
            logger.info("Dimensione del punto corretta "); // DEBUG

            Point p = null;
            try {
                p = Point.parseString(value.toString());
            } catch (Exception e) {
                e.printStackTrace();
                logger.debug("Errore parsing punto in map: " + e.getMessage());
                return; // check later if we have to exit or not
            }

            // calculate the distance between p and the centroids
            int id = 0;
            int clusterId = 0;
            double minDistance = -1;

            logger.info("Calcolo distanza tra p e centroidi"); // DEBUG

            for (Point c : centroids) {
                double distance = p.computeDistance(c);
                if (minDistance == -1 || distance < minDistance) {
                    minDistance = distance;
                    clusterId = id;
                }
                id++;
            }
            IntWritable it = new IntWritable(clusterId);
            WritableWrapper wrwr = new WritableWrapper(p, D);
            logger.debug(it.toString()); // DEBUG
            logger.debug(wrwr.toString()); // DEBUG
            context.write(it, wrwr);
            // key : clusterId, value : <point, centroids dimension, one>
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable, Iterable<WritableWrapper>, IntWritable, WritableWrapper> {
        // out : key - clusterId, values - <Point, centroids dimension, Denominator>

        private Logger logger = Logger.getLogger(KMeansReducer.class);

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
                    logger.debug("Dimensione punto in reduce: " + denominator);
                    logger.debug("Punto iniziale: " + numerator.toString());
                }
                id++;
                try {
                    numerator.sum(val.getPoint());
                } catch (Exception e) {
                    System.out.println("WRN: Point dimension mismatch");
                }

                denominator += val.getOne();
                logger.debug("Numeratore: " + numerator.toString());
                logger.debug("Denominatore: " + denominator);
            }
            
            logger.debug("Numero punti letti nel reducer: " + id);
            context.write(key, new WritableWrapper(numerator, d, denominator));
        }
    }

    private static List<Point> getCentroidsRandom(int K, int numberOfLines, FileSystem fs, Path inputFile) {
        List<Point> centroids = new ArrayList<>(K);
        Set<Integer> indexesCentroids = new HashSet<>();
        System.out.println("Numero di punti: " + numberOfLines); // DEBUG
        while (indexesCentroids.size() < K) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, numberOfLines);
            indexesCentroids.add(randomNum % numberOfLines);
        }

        //

        System.out.println("Lista indici a caso: "); // DEBUG
        for (Integer l : indexesCentroids) // DEBUG
            System.out.println(l.toString()); // DEBUG
        try (FSDataInputStream inputStream = fs.open(inputFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            numberOfLines = 0;
            String line = reader.readLine();
            System.out.println(line);
            while (line != null) {
                if (indexesCentroids.contains(numberOfLines)) {
                    centroids.add(Point.parseString(line));
                    if (centroids.size() == K)
                        break;
                }
                ++numberOfLines;
                line = reader.readLine();
            }
        } catch (Exception ex) {
            System.out.println("Problem with reading file points in hadoop");
            System.exit(1);
        }
        System.out.println("Controllo punti centroidi presi a caso: "); // DEBUG
        for (Point p : centroids) // DEBUG
            System.out.println(p.toString()); // DEBUG
        return centroids;
    }

    private static int countLines(FileSystem fs, Path inputFile) {
        int numberOfLines = 0;
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
        return numberOfLines;
    }

    private static void updateCache(List<Point> c, FileSystem fs, Path cacheFile) {
        try (FSDataOutputStream outputStream = fs.create(cacheFile, true);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            System.out.println("Inizio update cache"); // DEBUG
            for (Point line : c) {
                writer.write(line.toString());
                writer.newLine();
            }
            System.out.println("Fine update cache"); // DEBUG
        } catch (Exception ex) {
            System.out.println("Problem with writing file centroids in hadoop");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // TODO - lasciare che l'utente possa passare dati in input

        // if (otherArgs.length < 3) {
        // System.err.println("Usage: kmeans k: <k parameter> <path-file> <output
        // file>");
        // System.exit(2);
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

        Path inputFile = new Path("data.txt");
        Path outputDir = new Path("output");
        Path outputFile = new Path("output" + outputFileName);
        Path cacheFile = new Path(cacheName);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        // generare k indici di riga da selezionare a caso
        // aprire uno stream dal file
        // prendere le righe e considerarle centroidi
        // salvare i centroidi in un file da usare come cache

        FileSystem fs = inputFile.getFileSystem(conf);

        int numberOfLines = countLines(fs, inputFile);
        List<Point> centroids = getCentroidsRandom(K, numberOfLines, fs, inputFile);
        // all centroids are taken from the list of points
        updateCache(centroids, fs, cacheFile);
        // add file cache
        job.addCacheFile(cacheFile.toUri());
        // populate list new Centroids
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
                    System.out.println("Inizio parsing nuovo centroide"); // DEBUG
                    clusterId.readFields(inputStream);
                    wr.readFields(inputStream);

                    Point centroideNuovo = wr.getPoint(); // da popolare con il centroide
                    System.out.println("Nuovo centroide appena letto dalla cache: "); // DEBUG
                    System.out.println(centroideNuovo.toString()); // DEBUG
                    centroideNuovo.divide(wr.getOne());
                    newCentroids.set(clusterId.get(), centroideNuovo);
                }

            } catch (Exception ex) {
                System.out.println("Problem with reading new centroids in hadoop");
                System.out.println(ex.getMessage());
                System.exit(1);
            }
            // check conditions
            System.out.println("Numero nuovi centroidi: "); // DEBUG
            System.out.println(newCentroids.size()); // DEBUG
            System.out.println("Lista centroidi output reduce:"); // DEBUG
            for (Point c : newCentroids) // DEBUG
                System.out.println(c.toString()); // DEBUG

            stop = true;
            for (int i = 0; i < K; i++) {
                Point centroid = centroids.get(i);
                if (!centroid.equals(newCentroids.get(i))) {
                    stop = false;
                    // swap newCentroids and centroids
                    List<Point> temp = centroids;
                    centroids = newCentroids;
                    newCentroids = temp;

                    // reset newCentroids for the next iteration
                    for (int j = 0; j < K; ++j) {
                        newCentroids.set(j, null);
                    }
                    updateCache(centroids, fs, cacheFile);
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
