package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

class Point implements Writable {

    private double[] coordinates;

    private Point() {}

    public Point(int d) {
        if (d <= 0)
            throw new IllegalArgumentException("Invalid dimension for Point");

        coordinates = new double[d];
    }

    public Point(double[] a) {
        if (a == null || a.length <= 0)
            throw new IllegalArgumentException("Invalid array for Point");

        coordinates = a;
    }

    public Point(final Point p) {
        coordinates = Arrays.copyOf(p.coordinates, p.coordinates.length);
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
        return coordinates.length;
    }

    public void sum(Point that) {
        for (int i = 0; i < coordinates.length; i++)
            this.coordinates[i] += that.coordinates[i];
    }

    public void divide(int scalar) {
        for (int i = 0; i < coordinates.length; i++)
            this.coordinates[i] = this.coordinates[i] / scalar;
    }

    public double computeSquaredDistance(Point that) {

        double sum = 0;

        for (int i = 0; i < coordinates.length; i++)
            sum += Math.pow(this.coordinates[i] - that.coordinates[i], 2);

        return sum;
    }

    public double computeSquaredNorm() {
        double squaredNorm = 0;

        for (int i = 0; i < this.coordinates.length; i++)
            squaredNorm += this.coordinates[i] * this.coordinates[i];

        return squaredNorm;
    }

    @Override
    public String toString() {
        String str = "";
        for (int i = 0; i < coordinates.length - 1; i++) {
            str += Double.toString(this.coordinates[i]);
            str += " ";
        }
        str += Double.toString(this.coordinates[this.coordinates.length - 1]);
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
        for (int i = 0; i < coordinates.length; i++)
            if (this.coordinates[i] != that.coordinates[i])
                return false;

        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new ArrayPrimitiveWritable(coordinates).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable();
        apw.readFields(in);
        coordinates = (double[]) apw.get();
    }

    public static Point read(DataInput in) throws IOException {
        Point p = new Point();
        p.readFields(in);
        return p;
    }

}
