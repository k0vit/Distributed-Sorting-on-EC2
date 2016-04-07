package edu.hadoop.a9.common;

import java.util.ArrayList;
import java.util.Arrays;

//TODO use json simple
public class Distribution {
	public Distribution() {
		minTemp = Double.MAX_VALUE;
		maxTemp = Double.MIN_VALUE;
	}
	
	@Override
	public String toString() {
		return "Distribution [minTemp=" + minTemp + ", maxTemp=" + maxTemp
				+ ", samples=" + samples + "]";
	}

	public Distribution(double minTemp, double maxTemp) {
		super();
		this.minTemp = minTemp;
		this.maxTemp = maxTemp;
	}

	public String toJson() {
		return String.format("{\"min\":\"%d\", \"max\" : \"%d\" , \"samples\" : [%s] }", minTemp, maxTemp, Arrays.toString(samples.toArray()));
	}
	
	public double getMinTemp() {
		return minTemp;
	}
	public void setMinTemp(double minTemp) {
		this.minTemp = minTemp;
	}
	public double getMaxTemp() {
		return maxTemp;
	}
	public void setMaxTemp(double maxTemp) {
		this.maxTemp = maxTemp;
	}
	public ArrayList<Double> getSamples() {
		return samples;
	}
	public void setSamples(ArrayList<Double> samples) {
		this.samples = samples;
	}

	private double minTemp;
	private double maxTemp;
	private ArrayList<Double> samples = new ArrayList<Double>();
}
