package edu.hadoop.a9.common;

import java.util.ArrayList;
import java.util.Arrays;

public class Distribution {
	public Distribution() {
		minTemp = Integer.MAX_VALUE;
		maxTemp = Integer.MIN_VALUE;
	}
	
	@Override
	public String toString() {
		return "Distribution [minTemp=" + minTemp + ", maxTemp=" + maxTemp
				+ ", samples=" + samples + "]";
	}

	public Distribution(int minTemp, int maxTemp) {
		super();
		this.minTemp = minTemp;
		this.maxTemp = maxTemp;
	}

	public String toJson() {
		return String.format("{\"min\":\"%d\", \"max\" : \"%d\" , \"samples\" : [%s] }", minTemp, maxTemp, Arrays.toString(samples.toArray()));
	}
	
	public int getMinTemp() {
		return minTemp;
	}
	public void setMinTemp(int minTemp) {
		this.minTemp = minTemp;
	}
	public int getMaxTemp() {
		return maxTemp;
	}
	public void setMaxTemp(int maxTemp) {
		this.maxTemp = maxTemp;
	}
	public ArrayList<Integer> getSamples() {
		return samples;
	}
	public void setSamples(ArrayList<Integer> samples) {
		this.samples = samples;
	}

	private int minTemp;
	private int maxTemp;
	private ArrayList<Integer> samples = new ArrayList<Integer>();
}
