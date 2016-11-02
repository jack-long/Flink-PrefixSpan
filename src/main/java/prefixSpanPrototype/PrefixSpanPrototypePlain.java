package prefixSpanPrototype;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class PrefixSpanPrototypePlain {

	/**
	 * Testing the PrefixSpanPrototype with a plain input file
	 * @param args input file, output file, threshold, verbose level
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length == 4){
			List<int[]> data = new ArrayList<int[]>();
			//Read the data from the file
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			try{
				String line = br.readLine();
				while(line != null){
					String[] splitted = line.split(",");
					int[] sequence = new int[splitted.length-1];
					for(int i=1; i<splitted.length; i++){
						sequence[i-1] = Integer.parseInt(splitted[i]);
					}
					data.add(sequence);
					line = br.readLine();
				}
			}finally{
				br.close();
			}
			
			//run algorithm
			long threshold = (long)Math.ceil(Double.parseDouble(args[2]) * data.size());
			System.out.println("Number of sequences: " +data.size());
			System.out.println("Threshold: " + threshold);
			PrefixSpanPrototype prefixSpan = new PrefixSpanPrototype(data, threshold, Integer.parseInt(args[3]));
			prefixSpan.run();
			Map<int[],Long> frequentPatterns = sortByComparator(prefixSpan.getFrequentPatterns());
			
			//file output
			PrintStream ps = new PrintStream(new FileOutputStream(args[1]));
			ps.println("Result for PrefixSpanPrototypePlain.jar "+args[0]+ " "+args[1] + " " + args[2] + " " + args[3]);
			for(Map.Entry<int[], Long> cursor : frequentPatterns.entrySet()){
				for(int item : cursor.getKey()){
					ps.print(item+" ");
				}
				ps.println(" : " + cursor.getValue());
			}
			ps.close();
		}
		else{
			System.out.println("Error");
			System.out.println("PrefixSpanPrototypePlain input file, output file, minSupport, verbose level");
		}
	}
	
	/**
	 * Sorts the Map according to its value DESC
	 * @param unsortedMap
	 * @return sortedMap
	 */
	private static Map<int[],Long> sortByComparator(Map<int[],Long> unsortedMap){
		List<Entry<int[],Long>> list = new LinkedList<Entry<int[],Long>>(unsortedMap.entrySet());
		
		Collections.sort(list, new Comparator<Entry<int[],Long>>(){
			@Override
			public int compare(Entry<int[], Long> arg0, Entry<int[], Long> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});
		
		Map<int[],Long> sortedMap = new LinkedHashMap<int[],Long>();
		for(Entry<int[],Long> entry : list){
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
