package it.unipi.hadoop;

import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

public class Point {

    private double[] point;

    public Point(int d) {
        if (d <= 0)
            throw new IllegalArgumentException("Invalid dimension");

        point = new double[d];
    }

    public Point(double[] a) {
        point = a;
    }

    public static Point parseString(String line) throws Exception {
        StringTokenizer itr = new StringTokenizer(line);

        int d = itr.countTokens();

        if (d <= 0)
            throw new Exception("Invalid point string");

        double array[] = new double[d];

        int i = 0;
        while (itr.hasMoreTokens()) {
            array[i] = Double.parseDouble(itr.nextToken());
            // NullPointerException if the string is null
            // NumberFormatException if the string does not contain a parsable double.
            i++;
        }

        return new Point(array);
    }

    public int getDimension() {
        return point.length;
    }

    public void sum(Point that) throws Exception {

        if (that.getDimension() != this.getDimension())
            throw new Exception("Point dimension mismatch");

        for (int i = 0; i < point.length; i++)
            this.point[i] += that.point[i];
    }

    public void divide(int scalar) throws ArithmeticException {
        for (int i = 0; i < point.length; i++)
            this.point[i] = this.point[i] / scalar;
    }

    public double computeDistance(Point that) {

        double distance = 0;

        for (int i = 0; i < getDimension(); i++)
            distance += Math.pow(this.point[i] - that.point[i], 2);

        distance = Math.sqrt(distance);

        return distance;
    }

    public ArrayPrimitiveWritable getWritable() {
        return new ArrayPrimitiveWritable(point);
    }

    public void print() {
        System.out.println(toString());
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

    /*
     * public boolean compare(Point that) { if (this.point.length !=
     * that.point.length) return false;
     * 
     * for (int i = 0; i < point.length; i++) if (this.point[i] != that.point[i])
     * return false;
     * 
     * return true; }
     */
}