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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
    
    private static final double convergeDist = 1e-9;
    private static final int maxIterations = 100;

    private static String outputName = "centroids.txt";
    private static String inputName = "data.txt";
    private static String KMeansDirName = "KMeansTemporary";
    //part-00000 or part-r-00000 are ok
    private static final Pattern patternOutputFiles = Pattern.compile("part-(r-)?[\\d]{5}");

    static List<Path> getOutputFiles(FileSystem fs, Path outputDir) throws Exception {
        List<Path> listPaths = new ArrayList<>();
        Matcher matcher;
        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(outputDir, false);
        while (rit.hasNext()){
            Path outputFile = rit.next().getPath();
            matcher = patternOutputFiles.matcher(outputFile.toString());
            if (matcher.find()){
                listPaths.add(outputFile);
            }
        }
        return listPaths;
    }

    static List<Point> getRandomCentroids(Configuration conf, int k, FileSystem fs, Path inputFile, Path outputDir) throws Exception {

        List<Point> centroids = new ArrayList<>(k);
        int i = 0;
        //simple control, just in case
        if (fs.exists(outputDir)){
            fs.delete(outputDir, true);
        }
        while (i < k){
            Job job = UniformSampling.createUniformSamplingJob(conf, inputFile, outputDir, 1);
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
    
    static void updateCache(List<Point> centroids, FileSystem fs, Path cache) {

        try (FSDataOutputStream stream = fs.create(cache, true);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream))) {

            // write each centroid in a line of the cache file
            for (Point centroid : centroids) {
                writer.write(centroid.toString());
		writer.newLine();
            }

        } catch (IOException e) {
            // TODO handle exception
		System.err.println("Problem while updating the cache");
		System.err.println(e.getMessage());
        }
    }
    
    static void readCentroidsFromOutput(List<Point> list, FileSystem fs, Path outputDir) throws Exception{

        list.clear();

        WritableWrapper wrapper;
        List<Path> listOutputFiles = getOutputFiles(fs, outputDir);
        for (Path file: listOutputFiles){
            try (FSDataInputStream fileStream = fs.open(file);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream))) {

                String line = null;
                while ((line = reader.readLine()) != null) {

                    String[] split = line.split("\t"); // split key and value
                    Integer position = Integer.parseInt(split[0]); //key in the list
                    String value = split[1]; // get wrapper string

                    wrapper = WritableWrapper.parseString(value);

                    // divide to compute mean point
                    int count = wrapper.getCount();
                    Point centroid = wrapper.getPoint();
                    centroid.divide(count);

                    // Don't know if I can trust the position of the files for the new centroids
                    //Important: I assume that a key starts from 0 (really important)
                    while (list.size() <= position){
                        list.add(null);
                    }
                    list.set(position, centroid);
                }

            } catch (IOException | ParseException e) {
                // TODO handle exception
                System.err.println("Exception: " + e.getMessage());
            }
        }

        // TODO maybe check if centroids are not k
    }

    static double compareCentroids(List<Point> oldCentroids, List<Point> newCentroids) {
        // precondizione: i centroidi sono, rispetto alla singola posizione,
        // la versione precedente e la versione nuova --> stessa dimensione e stesso numero

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
    public static void main(String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        /*  
            List of arguments = [dimensionPoints numberCentroids pointsPath finalCentroidsPath];
            otherArgs[0] = d;
            otherArgs[1] = k;
            otherArgs[2] = inputName
            otherArgs[3] = outputName
            KMeansSamplingDirName può essere construito concatenando KMeansDir con "Sampling"
        */
        int d = 6; // default
        int k = 3; // default
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
        
        long startTime = System.currentTimeMillis();


        Configuration conf = new Configuration();
        Path inputFile = new Path(inputName);
        Path outputFile = new Path(outputName);
        Path KMeansDir = new Path(KMeansDirName);
        Path KMeansSamplingDir = new Path(KMeansDirName + "UniformSampling");
        FileSystem fs = FileSystem.get(conf);

	// cleanup everything there is as output
	if (fs.exists(outputFile)){
	    fs.delete(outputFile, true);
	}
        // select initial centroids and write them to file
	List<Point> centroids = getRandomCentroids(conf, k, fs, inputFile, KMeansSamplingDir);
        updateCache(centroids, fs, outputFile);
        boolean success = true;
        int iteration = 1;
        List<Point> currentCentroids = centroids;
        List<Point> newCentroids = new ArrayList<>(k);


        // delete the output directory if exists
        if (fs.exists(KMeansDir))
            fs.delete(KMeansDir, true);
        
        double tempDistance = convergeDist + 0.1;            
        long timeAlgStart = System.currentTimeMillis();

        while (success && tempDistance > convergeDist && iteration <= maxIterations) {
            
            // new job to compute new centroids
            Job kmeansJob = KMeans.createKMeansJob(conf, inputFile, KMeansDir, outputFile, d, k);
            success = kmeansJob.waitForCompletion(true);
            // add new centroids to list
            readCentroidsFromOutput(newCentroids, fs, KMeansDir);

            // DEBUG:
            // System.out.println("New centroids " + iteration + ":");
            // for (Point point : newCentroids)
            //     System.out.println("\t" + point);

            // write new centroids to file read from mapper
            updateCache(newCentroids, fs, outputFile);

            // sum of all the relative errors between old centroids and new centroids
            tempDistance = compareCentroids(currentCentroids, newCentroids);
            
            // before next iteration update old centroids list
            List<Point> tempList = currentCentroids;
            currentCentroids = newCentroids;
            newCentroids = tempList;

            iteration++;
            // System.out.println("tempDistance = " + Double.toString(tempDistance)); // DEBUG
            // delete the output directory if exists
            if (fs.exists(KMeansDir))
                fs.delete(KMeansDir, true);
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Tempo di inizio: " + startTime);
        System.out.println("Tempo inizio dell'algoritmo: " + timeAlgStart ); 
        System.out.println("Tempo di fine: " + endTime);
        System.out.println("Durata dell'algoritmo: " + (endTime - startTime));

        System.out.println("Numero di iterazioni: "+ iteration);
        System.exit(success ? 0 : 1);
    }
}
