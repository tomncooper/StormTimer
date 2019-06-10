package uk.org.tomcooper.stormtimer.topology;

import java.util.Random;

public class KeyGenerator {

	private static String[] keys = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"};
	private static Double[] weights = {(6.0/32.0), (4.0/32.0), (3.0/32.0), (3.0/32.0), (2.0/32.0), (2.0/32.0),  
								       (2.0/32.0), (2.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0), 
								       (1.0/32.0), (1.0/32.0), (1.0/32.0), (1.0/32.0)};
	private Random random;
	private Double[] cumulativeWeights;
	
	public KeyGenerator() {
		random = new Random();
		cumulativeWeights = new Double[weights.length];
		cumulativeWeights[0] = weights[0];
		for(int i = 1; i < cumulativeWeights.length; i++){
			cumulativeWeights[i] = cumulativeWeights[i-1] + weights[i];
		}
	}

	public String chooseKey() {
		
		double rand = random.nextDouble();
		for(int i = 0; i < cumulativeWeights.length; i++) {
		
			if(cumulativeWeights[i] >= rand) {
				return keys[i];
			}
			
		}
		return "A";
		
	}
}
