package it.unipi.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io;
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
    private static final int D = 0;
    private static final String cacheName = "centroids.txt";
    private static final String outputFileName = "/part-00000";
    private final static IntWritable one = new IntWritable(1);

    public static class Point implements Writable {

        private List<Double> point = new ArrayList<>(D);
        private int clusterId = -1;
        private double minDistance = -1;
        
        public Point(String line) {
            StringTokenizer itr = new StringTokenizer(line);

            if (itr.countTokens() != D) {
                throw new Exception("Elements of point different from D!")
                return;
            }
            
            while (itr.hasMoreTokens()) {
                point.add( Double.parseDouble( itr.nextToken()) );
                // NullPointerException - if the string is null
                // NumberFormatException - if the string does not contain a parsable double.
            }
        }

        public int getClusterId() {
            return clusterId;
        }

        public void assignToCluster(int clusterId) {
            this.clusterId = clusterId;
        }

        public void computeDistance(Point that, int clusterThat) {

            double distance = 0;

            for (int i = 0; i < D; i++)
                distance += Math.pow(this.point.get(i) - that.point.get(i), 2);

            distance = Math.sqrt(distance);
            
            if(minDistance == -1 || distance < minDistance)
            {
                minDistance= distance;
                assignToCluster(clusterThat);
            }
                     
        }
    }

    public void map(final Object key, final Text value, final Context context)  throws IOException, InterruptedException {
        ArrayList<Point> centroids = new ArrayList<Point>();
        
        File file = new File( context.getCacheFile() );
        FileReader fr = new FileReader(file); 
        BufferedReader br = new BufferedReader(fr);
        String line;

        while((line = br.readLine()) != null){
            centroids.append( new Point( line ) );            
        }
        
        Point p = new Point( value.toString() );
    
        // calculate the distance between p and the centroids
        int id = 0;
        for( Point c : centroids ){
            p.computeDistance( c , id);
            id ++ ;
        }
    
        outputKey.set( new IntWritable( p.getClusterID() );
        outputValue.set(p, one);

        context.write(outputKey, outputValue);
        }
    }
  
    public static class KMeansReducer
         extends Reducer<Text,Text,NullWritable,Text> {
      private Text result ;
  
      public void reduce(Text key, Iterable<Text> values,
                         Context context
                         ) throws IOException, InterruptedException {
        int sum = 0;
        int countPoint = 0;
        String[] value;
        for( Text val: values)
        {
            value = val.toString().split(",");
            int valuePoint = Integer.parseInt(value[0]);
            int onePoint = Integer.parseInt(value[1]);
            sum += valuePoint;
            countPoint += onePoint;
        }
        result = new Text(Integer.toString(sum)+","+Integer.toString(countPoint));
        context.write(key, result);
      }
    }
  
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: kmeans k: <k parameter> <path-file> <output file>");
            System.exit(2);
        }

        int K = Integer.parseInt( otherArgs[ K_INDEX ]  ); 

        Job job = Job.getInstance(conf, "k means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        Path inputFile = new Path( otherArgs[INPUT_INDEX] );
        Path outputDir = new Path( otherArgs[OUTPUT_INDEX] );
        Path outputFile = new Path( otherArgs[OUTPUT_INDEX] + outputFileName);
        Path cacheFile = new Path(cacheName);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);
        
        
        //generare k indici di riga da selezionare a caso
        //aprire uno stream dal file
        //prendere le righe e considerarle centroidi
        //salvare i centroidi in un file da usare come cache
        Set<Long> indexesCentroids = new HashSet<>();
        Random rand = new Random();
        List<String> centroids = new ArrayList<>();
        FileSystem fs = inputFile.getFileSystem(conf);

        long numberOfLines = Files.lines(inputFile).count();
        while (indexesCentroids.size() < K){
            long valueRandom = rand.nextLong();
            if (valueRandom < numberOfLines)
                indexesCentroids.add(valueRandom); 
        }  

        try (FSDataInputStream inputStream = fs.open(inputFile);
         BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)))
        {
            numberOfLines = 0;
            String line = reader.nextLine();
            while (line != null) {
                if (indexesCentroids.contains(numberOfLines))
                    centroids.add(line);
                ++numberOfLines;
                line = reader.nextLine();
            }
        }
        catch (Exception ex){System.out.println("Problem with reading file points in hadoop"); System.exit(1);}

        //all centroids are taken from the list of points
        
        try (FSDataOutputStream outputStream = fs.create(cacheFile); 
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream)))
        {
            for (String line : centroids){
                writer.write(line, 0, line.length());
                writer.newLine();
            }
        }
        catch (Exception ex){System.out.println("Problem with writing file centroids in hadoop"); System.exit(1);}

        //add file cache 
        job.addFileCache(new URI(cacheName));

        while (true){
            job.waitForCompletion(true);
            // find centroids
            try (FSDataInputStream inputStream = fs.open(outputFile);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)))
            {
                //output file : id cluster(index in cache file), somma e numero di punti
                //in questo loop recuperare i nuovi centroidi e poi conservarli in una lista
                //Dopo di ciò confrontare i vecchi centroidi con i nuovi centroidi: se sono uguali, break dal loop while
                //Se non sono uguali, allora aggiornare i centroidi nel file cache
            }
            catch (Exception ex){System.out.println("Problem with reading file points in hadoop"); System.exit(1);}

            // update centroids
        }

        
    }
  }





