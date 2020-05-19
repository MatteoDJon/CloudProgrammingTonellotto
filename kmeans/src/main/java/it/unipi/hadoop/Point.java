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

    public Point(String line) throws Exception {
        StringTokenizer itr = new StringTokenizer(line);
        
        int d = itr.countTokens();

        if (d <= 0)
            throw new Exception("Invalid point");

        point = new double[d];
        
        int i = 0;
        while (itr.hasMoreTokens()) {
            point[i] = Double.parseDouble(itr.nextToken());
            // NullPointerException if the string is null
            // NumberFormatException if the string does not contain a parsable double.
            i++;
        }
    }

    public int getDimension() {
        return point.length;
    }

    public void sum(Point that) throws Exception {

        if ( that.getDimension() != this.getDimension() )
            throw new Exception("Point dimension mismatch");

        for (int i = 0; i < point.length; i++)
            this.point[i] += that.point[i];
    }
    
    public void divide( int denominator ) throws ArithmeticException{
        for (int i = 0; i < point.length; i++)
            this.point[i] = this.point[i] / denominator;
    }
    
    public boolean compare( Point p ){
        if( this.point.length != p.getDimension() )
            return false;
         
        for (int i = 0; i < point.length; i++)
            if ( this.point[i] != p.point[i] )
                return false;
        
        return true;
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

    public void print(){
        String toPrint="";
        for (int i = 0; i < point.length; i++)
            toPrint+=Double.toString(this.point[i]);
            toPrint+=" ";
        System.out.println(toPrint);
    }
}