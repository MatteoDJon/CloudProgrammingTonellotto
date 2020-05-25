package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class WritableWrapper implements Writable {

    private Point p;
    private IntWritable count = new IntWritable();

    public WritableWrapper() {
        
    }

    public WritableWrapper(Point p) {
        this(p, 1);
    }

    public WritableWrapper(Point p, int count) {
        this.p = p;
        this.count.set(count);
    }

    public Point getPoint() {
        return p;
    }

    public WritableWrapper setPoint(Point p) {
        this.p = p;
        return this;
    }

    public int getCount() {
        return count.get();
    }

    public WritableWrapper setCount(int count) {
        this.count.set(count);
        return this;
    }

    @Override
    public String toString() {
        return "WritableWrapper(" + count + ")[" + p + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        p.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        p = Point.read(in);
        count.readFields(in);
    }

    public static WritableWrapper read(DataInput in) throws IOException {
        WritableWrapper w = new WritableWrapper();
        w.readFields(in);
        return w;
    }

}
