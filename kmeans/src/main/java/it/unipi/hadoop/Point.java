package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

class Point implements Writable {

    private double[] point;

    private Point() {}

    public Point(int d) {
        if (d <= 0)
            throw new IllegalArgumentException("Invalid dimension for Point");

        point = new double[d];
    }

    public Point(double[] a) {
        if (a == null || a.length <= 0)
            throw new IllegalArgumentException("Invalid array for Point");

        point = a;
    }

    public Point(final Point p) {
        point = Arrays.copyOf(p.point, p.point.length);
    }

    public static Point parseString(String line) throws ParseException {
        String[] itr = line.split(" ");

        int d = itr.length;

        if (d <= 0)
            throw new ParseException("Invalid string for Point", 0);

        double array[] = new double[d];

        int i = 0;
        for (String s : itr) {
            array[i] = Double.parseDouble(s);
            i++;
        }

        return new Point(array);
    }

    public int getDimension() {
        return point.length;
    }

    public void sum(Point that) {
        for (int i = 0; i < point.length; i++)
            this.point[i] += that.point[i];
    }

    public void divide(int scalar) {
        for (int i = 0; i < point.length; i++)
            this.point[i] = this.point[i] / scalar;
    }

    public double computeSquaredDistance(Point that) {

        double sum = 0;

        for (int i = 0; i < getDimension(); i++)
            sum += Math.pow(this.point[i] - that.point[i], 2);

        return sum;
    }

    public double computeSquaredNorm() {
        double squaredNorm = 0;

        for (int i = 0; i < this.point.length; i++)
            squaredNorm += this.point[i] * this.point[i];

        return squaredNorm;
    }

    @Override
    public String toString() {
        String str = "";
        for (int i = 0; i < point.length - 1; i++) {
            str += Double.toString(this.point[i]);
            str += " ";
        }
        str += Double.toString(this.point[this.point.length - 1]);
        return str;
    }

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o)
            return true;
        // null check
        if (o == null)
            return false;
        // type check and cast
        if (getClass() != o.getClass())
            return false;
        Point that = (Point) o;
        // field comparison
        for (int i = 0; i < point.length; i++)
            if (this.point[i] != that.point[i])
                return false;

        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new ArrayPrimitiveWritable(point).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable();
        apw.readFields(in);
        point = (double[]) apw.get();
    }

    public static Point read(DataInput in) throws IOException {
        Point p = new Point();
        p.readFields(in);
        return p;
    }

}
