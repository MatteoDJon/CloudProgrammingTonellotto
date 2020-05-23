package it.unipi.hadoop;

import java.io.*;
import org.apache.hadoop.io.*;

public class WritableWrapper implements Writable {

    private Point p;
    private IntWritable one = new IntWritable(1);

    private WritableWrapper() {
        
    }

    public WritableWrapper(Point p) {
        this.p = p;
    }

    public Point getPoint() {
        return p;
    }

    public int getOne() {
        return one.get();
    }

    @Override
    public String toString() {
        return "WritableWrapper [" + p + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        p.write(out);
        one.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        p = Point.read(in);
        one.readFields(in);
    }

    public static WritableWrapper read(DataInput in) throws IOException {
        WritableWrapper w = new WritableWrapper();
        w.readFields(in);
        return w;
    }

}
