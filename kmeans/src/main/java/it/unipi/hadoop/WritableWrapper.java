package it.unipi.hadoop;

import java.io.*;
import org.apache.hadoop.io.*;

public class WritableWrapper implements Writable {
    private Point p;
    private IntWritable one = new IntWritable(1);
    private IntWritable dim = new IntWritable(-1);

    public WritableWrapper() {}

    public WritableWrapper(Point p, int d){
        this.p = p;
        dim = new IntWritable( d );
    }

    public WritableWrapper(Point p, int d, int sum){
        this.p = p;
        dim = new IntWritable( d );
        one = new IntWritable( sum );
    }

    public int getDimension(){ return dim.get(); }
    public Point getPoint(){ return p; }
    public int getOne(){ return one.get(); }

    public void write(DataOutput out) throws IOException {
        p.getWritable().write(out);
        one.write(out);
        dim.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        ArrayPrimitiveWritable apw = p.getWritable();
        apw.readFields(in);
        double[] array = (double[]) (double[]) apw.get();
        p = new Point(array);

        one.readFields(in);
        dim.readFields(in);
    }
}
