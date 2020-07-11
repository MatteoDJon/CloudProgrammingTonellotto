package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

class WritableWrapper implements Writable {

    private Point point;
    private int count;

    private static IntWritable wCount = new IntWritable();

    public WritableWrapper() {

    }

    public WritableWrapper(Point point) {
        this(point, 1);
    }

    public WritableWrapper(Point point, int count) {
        this.point = point;
        this.count = count;
    }

    public Point getPoint() {
        return point;
    }

    public WritableWrapper setPoint(Point point) {
        this.point = point;
        return this;
    }

    public int getCount() {
        return count;
    }

    public WritableWrapper setCount(int count) {
        this.count = count;
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        wCount.set(count);
        wCount.write(out);
        point.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wCount.readFields(in);
        count = wCount.get();
        point = Point.read(in);
    }

    public static WritableWrapper read(DataInput in) throws IOException {
        WritableWrapper w = new WritableWrapper();
        w.readFields(in);
        return w;
    }

    @Override
    public String toString() {
        return "(" + count + ") [" + point + "]";
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
