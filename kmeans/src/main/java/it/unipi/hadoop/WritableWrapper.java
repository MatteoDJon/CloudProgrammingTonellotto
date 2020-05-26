package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;
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
    public void write(DataOutput out) throws IOException {
        count.write(out);
        p.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count.readFields(in);
        p = Point.read(in);
    }

    public static WritableWrapper read(DataInput in) throws IOException {
        WritableWrapper w = new WritableWrapper();
        w.readFields(in);
        return w;
    }

    @Override
    public String toString() {
        return "(" + count + ") [" + p + "]";
    }

    public static WritableWrapper parseString(String wrapperString) throws ParseException {
        String[] split = wrapperString.split(" ", 2);
        String countString = StringUtils.substringBetween(split[0], "(", ")");
        String pointString = StringUtils.substringBetween(split[1], "[", "]");

        int count = Integer.parseInt(countString);
        Point point = Point.parseString(pointString);

        return new WritableWrapper(point, count);
    }

}
