package it.unipi.hadoop;

import java.io.*;
import org.apache.hadoop.io.*;

public class WritableWrapper implements Writable {

    private Point p;

    public WritableWrapper() {
        
    }

    public WritableWrapper(Point p) {
        this.p = p;
    }

    public Point getPoint() {
        return p;
    }

    public WritableWrapper setPoint(Point p) {
        this.p = p;
        return this;
    }

    @Override
    public String toString() {
        return "WritableWrapper[" + p + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        p.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        p = Point.read(in);
    }

    public static WritableWrapper read(DataInput in) throws IOException {
        WritableWrapper w = new WritableWrapper();
        w.readFields(in);
        return w;
    }

}
