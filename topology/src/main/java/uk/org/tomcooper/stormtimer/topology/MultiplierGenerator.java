package uk.org.tomcooper.stormtimer.topology;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class MultiplierGenerator {
	
	int mean;
	double std;
	int[] values;
	double[] weights;
	NormalDistribution gaussian;
	UniformRealDistribution uniform;

	public MultiplierGenerator(int min, int max, int mean, double std) {
		
		gaussian = new NormalDistribution((double)mean, std);
		uniform = new UniformRealDistribution();

		int numvalues = (max-min) + 1;
		values = new int[numvalues];
		weights = new double[numvalues];
		for (int i=0; i<=values.length-1; i++) {
			int value = min+ i;
			values[i] = value;
			weights[i] = gaussian.cumulativeProbability((double) value);
		}
	
	}
	
	public int generateMultiplier() {
		double sample = uniform.sample();
		for(int i = 0; i<=weights.length-1; i++) {
			if(weights[i] >= sample){
				return values[i];
			}
		}
		return mean;
	}

}
