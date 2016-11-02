package prefixSpanPrototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the PrefixSpan Algorithm in plain JAVA.
 * @author Jens Röwekamp
 *
 */
public class PrefixSpanPrototype {

	/**
	 * Main Function, which defines a sample database and starts the algorithm.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		List<int[]> data = new ArrayList<int[]>();
		//first sample database for testing
		data.add(new int[]{0,1,0,1,2,3,0,1,3,0,4,0,3,6,0}); //a(abc)(ac)d(cf)
		data.add(new int[]{0,1,4,0,3,0,2,3,0,1,5,0});		//(ad)c(bc)(ae)
		data.add(new int[]{0,5,6,0,1,2,0,4,6,0,3,0,2,0});	//(ef)(ab)(df)cb
		data.add(new int[]{0,5,0,7,0,1,6,0,3,0,2,0,3,0});	//eg(af)cbc
		
		//second sample database for testing
//		data.add(new int[]{0,1,2,3,4,0});					//(abcd)
//		data.add(new int[]{0,1,2,4,0});						//(abd)
		
		//third sample database for testing
//		data.add(new int[]{0,1,0,1,2,3,0,4,0});				//a(abc)d
//		data.add(new int[]{0,4,5,0,1,3,0,2,6,0});			//(de)(ac)(bf)
		
		
		PrefixSpanPrototype prefixSpan = new PrefixSpanPrototype(data,2L,0);
		prefixSpan.run();
		
		Map<int[],Long> frequentPatterns = prefixSpan.getFrequentPatterns();
		
		//result output
		for(Map.Entry<int[], Long> cursor : frequentPatterns.entrySet()){
			for(int item : cursor.getKey()){
				System.out.print(item+" ");
			}
			System.out.println(" : " + cursor.getValue());
		}
	}

	private List<int[]> db;
	private long threshold;
	private int verbose;
	private HashMap<int[],Long> frequentPatterns = new HashMap<int[],Long>();
	private int[] prefixPointer;
	private int lastPrefixItem = -1;
	
	/**
	 * Constructor.
	 * @param database to be checked. 0 is delimiter.
	 * @param threshold to be reached to be frequent
	 * @param verbose level of information output. 0 - nothing, 1 - direct result output to std out, 2 - debug
	 */
	public PrefixSpanPrototype(List<int[]> db, long threshold, int verbose){
		this.db = db;
		this.threshold = threshold;
		this.verbose = verbose;
		int[] prefixPointer = new int[db.size()];
		for(int i = 0; i<prefixPointer.length; i++){
			prefixPointer[i] = 0;
		}
		this.prefixPointer = prefixPointer;
		if(verbose > 1){
			System.out.println("database:");
			for(int[] i : this.db){
				for(int j : i){
					System.out.print(j+" ");
				}
				System.out.println();
			}
		}
	}

	/**
	 * Constructor.
	 * @param database to be checked. 0 is delimiter.
	 * @param threshold to be reached to be frequent
	 * @param verbose level of information output. 0 - nothing, 1 - direct result output to std out, 2 - debug
	 * @param lastPrefixItem which should be considered when executed
	 */
	public PrefixSpanPrototype(List<int[]> db, long threshold, int verbose, int lastPrefixItem){
		List<int[]> tempDb = new ArrayList<int[]>();
		this.threshold = threshold;
		this.verbose = verbose;
		this.lastPrefixItem = lastPrefixItem;
		
		for(int[] seq : db){
			int[] newSeq = new int[seq.length+1];
			newSeq[0] = lastPrefixItem;
			for(int i=1, j=0; i<newSeq.length; i++, j++){
				newSeq[i] = seq[j];
			}
			tempDb.add(newSeq);
		}
		this.db = tempDb;
		
		int[] prefixPointer = new int[db.size()];
		for(int i = 0; i<prefixPointer.length; i++){
			prefixPointer[i] = 1;
		}
		this.prefixPointer = prefixPointer;
		
		if(verbose > 1){
			System.out.println("database of prefix :" + lastPrefixItem);
			for(int[] i : this.db){
				for(int j : i){
					System.out.print(j+" ");
				}
				System.out.println();
			}
		}
	}
	
	/**
	 * Execution of the algorithm.
	 * First finds the frequent length 1 patterns in the whole database and then recursively checks for frequent sub patterns.
	 * @throws Exception
	 */
	public void run() throws Exception{
		Map<Integer,Long> prefixPattern =  findLength1SequentialPatterns(prefixPointer);
		filterDb(prefixPattern); //filter out infrequent items from the database to reduce size of projections
		if(verbose > 1){
			System.out.println("filtered database:");
			for(int[] i : this.db){
				for(int j : i){
					System.out.print(j+" ");
				}
				System.out.println();
			}
		}
		for(Map.Entry<Integer, Long> cursor : prefixPattern.entrySet()){
			if(verbose > 1)System.out.println("-------------------------------------------------------------");
			if(verbose > 0)System.out.println("0 " + cursor.getKey() + " 0  : " + cursor.getValue());
			if(lastPrefixItem < 0)frequentPatterns.put(new int[]{0,cursor.getKey(),0}, cursor.getValue());
			else{
				if(cursor.getKey() > 0) frequentPatterns.put(new int[]{0,lastPrefixItem,0,cursor.getKey(),0}, cursor.getValue());
				else frequentPatterns.put(new int[]{0,lastPrefixItem,-cursor.getKey(),0}, cursor.getValue());
			}
			HashMap<int[],Long> frequentSubPatterns;
			if(lastPrefixItem < 0) frequentSubPatterns = findFrequentPatterns(new int[]{0,cursor.getKey()},prefixPointer.clone());
			else{
				int[] oldPrefix;
				if(cursor.getKey() > 0){
					oldPrefix = new int[4];
					oldPrefix[0] = 0;
					oldPrefix[1] = lastPrefixItem;
					oldPrefix[2] = 0;
					oldPrefix[3] = cursor.getKey();
				}else{
					oldPrefix = new int[3];
					oldPrefix[0] = 0;
					oldPrefix[1] = lastPrefixItem;
					oldPrefix[2] = -cursor.getKey();
				}
				frequentSubPatterns = findFrequentPatterns(oldPrefix,prefixPointer.clone());
			}
			frequentPatterns.putAll(frequentSubPatterns);
			frequentSubPatterns.clear();
		}
	}

	/**
	 * Finds the length 1 frequent sequential patterns.
	 * @param prefixPointer of the database to check from
	 * @return Map of <frequent length 1 pattern, frequency>
	 */
	private Map<Integer, Long> findLength1SequentialPatterns(int[] prefixPointer) {
		Map<Integer, Long> frequentSubSequences = new HashMap<Integer, Long>();
		Map<Integer, Long> tempCountMap = new HashMap<Integer, Long>();
		int prefixPointerCounter = 0;
		Integer prefix = null;
		boolean inItemSet;
		for(int[] sequence : db){
			if(prefixPointer[prefixPointerCounter] >= 0){ //if pointer isn't run out
				if(prefixPointer[prefixPointerCounter] > 0){ //n-th run
					prefix = sequence[prefixPointer[prefixPointerCounter]-1]; //set the prefix
				}else{ //first run
					prefix = null;
				}
				if(sequence[prefixPointer[prefixPointerCounter]] == 0){ //pointer is outside first itemset
					inItemSet = false;
				}else{ //pointer is inside first dataset
					inItemSet = true;
				}
				for(int c = prefixPointer[prefixPointerCounter]; c < sequence.length; c++){
					if(inItemSet){
						tempCountMap.put(-sequence[c], 1L); //add items in the first itemset as _item to tempCountMap
						if(sequence[c] == 0)inItemSet = false;
					}else{
						tempCountMap.put(sequence[c], 1L); //add items in other itemsets as item to tempCountMap
						if(prefix != null && sequence[c] == prefix && c < sequence.length-1){
							//add items which directly follow a found prefix as _item to tempCountMap
							for(int j=c+1; j < sequence.length; j++){
								if(sequence[j] == 0) break;
								tempCountMap.put(-sequence[j], 1L);
							}
						}
					}
				}
				//Add the occurrences of items of one sequence to the total counter frequentSubSequences
				for(Map.Entry<Integer, Long> cursor : tempCountMap.entrySet()){
					Long itemCount = frequentSubSequences.get(cursor.getKey());
					if (itemCount == null){
						frequentSubSequences.put(cursor.getKey(), 1L);
					}else{
						frequentSubSequences.replace(cursor.getKey(), ++itemCount);
					}
				}
				tempCountMap.clear();
			}
			prefixPointerCounter++;
		}
		//Remove items which don't reach the threshold
		ArrayList<Integer> removeList = new ArrayList<Integer>();
		for (Map.Entry<Integer, Long> cursor : frequentSubSequences.entrySet()) {
			if (cursor.getValue() < threshold) {
				removeList.add(cursor.getKey());
			}
		}
		for (Integer i : removeList) {
			frequentSubSequences.remove(i);
		}
		//Remove delimiting 0
		frequentSubSequences.remove(0);
		
		//debugging print out to terminal
		if(verbose > 1){
			System.out.println("length 1 counting:");
			for (Map.Entry<Integer, Long> cursor : frequentSubSequences.entrySet()) {
				System.out.println(cursor.getKey() + " : " + cursor.getValue());
			}
			System.out.println();
		}
		return frequentSubSequences;
	}
	
	/**
	 * Filters out the infrequent base items from the database to reduce projection size
	 */
	private void filterDb(Map<Integer,Long> frequentBaseItems){
		
		ArrayList<ArrayList<Integer>> newDb = new ArrayList<ArrayList<Integer>>();
		
		if(prefixPointer[0] == 0){ //first case: no prefix given
			for(int[] sequence : db){
				ArrayList<Integer> newSequence = new ArrayList<Integer>();
				for(Integer item : sequence){
					if(frequentBaseItems.containsKey(item) || item == 0) newSequence.add(item);
				}
				if(!newSequence.isEmpty()) newDb.add(newSequence);
			}
		}else{ //second case: prefix given. We've to ignore the first item of each sequence and consider negative values.
			for(int[] sequence : db){
				ArrayList<Integer> newSequence = new ArrayList<Integer>();
				newSequence.add(sequence[0]);
				for(int i=1; i<sequence.length; i++){
					if(frequentBaseItems.containsKey(sequence[i]) || sequence[i] == 0 || frequentBaseItems.containsKey(-sequence[i])) newSequence.add(sequence[i]);
				}
				if(newSequence.size()>1) newDb.add(newSequence);
			}
		}
		
		db.clear();
		for(ArrayList<Integer> sequence : newDb){
			int[] add = new int[sequence.size()];
			for(int i=0; i<add.length; i++){
				add[i] = sequence.get(i);
			}
			db.add(add);
		}
	}

	/**
	 * Recursive function which finds all frequent patterns and builds the frequent sub patterns.
	 * @param oldPrefix to build newPrefix
	 * @param oldPrefixPointer to build newPrefixPointer
	 * @return Map of frequentSubPatterns and their frequency
	 */
	private HashMap<int[],Long> findFrequentPatterns(int[] oldPrefix, int[] oldPrefixPointer) {
		HashMap<int[],Long> frequentPatterns = new HashMap<int[],Long>();
		
		int[] relevantItemSetPrefix = splitPrefix(oldPrefix); //splits the old prefix to get the relevant itemSetPrefix
		
		int[] newPrefixPointer = setPrefixPointer(relevantItemSetPrefix, oldPrefixPointer); //generates the projected DB
		
		Map<Integer, Long> prefixPatterns = findLength1SequentialPatterns(newPrefixPointer); //finds the new frequent subPattern item
		
		//builds the new frequent subPatterns from oldPrefix and prefixPatterns
		for(Map.Entry<Integer, Long> cursor : prefixPatterns.entrySet()){
			int[] newPrefix;
			int[] putPrefix;
			if(cursor.getKey()<0){ //inside itemset (_item) - directly append prefixPattern 0123X
				newPrefix = new int[oldPrefix.length+1];
				putPrefix = new int[oldPrefix.length+2];
				for(int i = 0; i<oldPrefix.length; i++){
					if(oldPrefix[i]>0){
						newPrefix[i] = oldPrefix[i];
						putPrefix[i] = oldPrefix[i];
					}else{
						newPrefix[i] = -oldPrefix[i];
						putPrefix[i] = -oldPrefix[i];
					}
				}
				newPrefix[oldPrefix.length] = -cursor.getKey();
				putPrefix[oldPrefix.length] = -cursor.getKey();
				putPrefix[oldPrefix.length+1] = 0;
			}else{ //outside itemset (item) - append prefixPattern in new itemset 01230X
				newPrefix = new int[oldPrefix.length+2];
				putPrefix = new int[oldPrefix.length+3];
				for(int i = 0; i<oldPrefix.length; i++){
					if(oldPrefix[i]>0){
						newPrefix[i] = oldPrefix[i];
						putPrefix[i] = oldPrefix[i];
					}else{
						newPrefix[i] = -oldPrefix[i];
						putPrefix[i] = -oldPrefix[i];
					}
				}
				newPrefix[oldPrefix.length] = 0;
				newPrefix[oldPrefix.length+1] = cursor.getKey();
				putPrefix[oldPrefix.length] = 0;
				putPrefix[oldPrefix.length+1] = cursor.getKey();
				putPrefix[oldPrefix.length+2] = 0;
			}
			frequentPatterns.put(putPrefix, cursor.getValue());
			if(verbose>0){
				for(int i : putPrefix){
					System.out.print(i+" ");
				}
				System.out.println(" : " + cursor.getValue());
			}
			//recursively calls itself until no frequent subPattern items are found
			HashMap<int[],Long> collectedResults = findFrequentPatterns(newPrefix, newPrefixPointer.clone());
			frequentPatterns.putAll(collectedResults);
		}
		return frequentPatterns;
	}
	
	/**
	 * Splits the old prefix at the last 0.
	 * @param oldPrefix which should be split
	 * @return itemset of the relevant prefix
	 */
	private int[] splitPrefix(int[] oldPrefix) {
		int last0pointer = 0;
		for(int i = oldPrefix.length-1; i>=0; i--){
			if(oldPrefix[i] == 0){
				last0pointer = i+1;
				break;
			}
		}
		int[] splittedPrefix = new int[oldPrefix.length-last0pointer];
		for(int i = 0; i<splittedPrefix.length; i++){
			splittedPrefix[i] = oldPrefix[last0pointer+i];
		}
		return splittedPrefix;
	}

	/**
	 * Projects the database by setting new pointers.
	 * @param relevant prefix to project database
	 * @param prefix pointer from where to start projecting
	 * @return new pointers of the projected database
	 */
	private int[] setPrefixPointer(int[] prefix, int[] prefixPointer) {
		if(prefix.length > 1){ //prefix is inside itemset, we've to project on the first occurrence of the whole pattern
			int prefixPointerCounter = 0;
			boolean[] itemsFound = new boolean[prefix.length];
			boolean itemFound;
			for(int[] sequence : db){
				itemFound = false;
				for(int i = 0; i<itemsFound.length; i++){
					itemsFound[i] = false;
				}
				if(prefixPointer[prefixPointerCounter] >= 0){
					if(prefixPointer[prefixPointerCounter] - (prefix.length -1) >= 0){
						prefixPointer[prefixPointerCounter] -= (prefix.length -1);
					}else{
						prefixPointer[prefixPointerCounter] = 0;
					}
					for(int c = prefixPointer[prefixPointerCounter]; c < sequence.length; c++){
						for(int i=0; i<prefix.length; i++){
							if(sequence[c] == prefix[i]){
								itemsFound[i]=true;
								int counter = 0;
								for(int d = 0; d < itemsFound.length; d++){
									if(itemsFound[d])counter++;
								}
								if(counter == prefix.length){
									itemFound = true;
									prefixPointer[prefixPointerCounter] = c+1; //set pointer to next item
									break;
								}
							}
							if(sequence[c] == 0){
								for(int d=0; d<itemsFound.length; d++){
									itemsFound[d] = false;
								}
							}
						}
					}
				}
				if(!itemFound){ //if prefix couldn't be found, set pointer to -1 to indicate termination
					prefixPointer[prefixPointerCounter] = -1;
				}
				prefixPointerCounter++;
			}
		}
		else{ //prefix is outside itemset, we have to split on the first occurrence of the item
			int prefixPointerCounter = 0;
			for(int[] sequence : db){
				if(prefixPointer[prefixPointerCounter] >= 0){
					boolean itemFound = false;
					//find next delimiter
					for(int c = prefixPointer[prefixPointerCounter]; c < sequence.length; c++){
						if (sequence[c] == 0){
							prefixPointer[prefixPointerCounter] = c;
							break;
						}
					}
					//determine position of prefix
					for(int c = prefixPointer[prefixPointerCounter]; c < sequence.length; c++){
						if (sequence[c] == prefix[0]){
							prefixPointer[prefixPointerCounter] = c;
							itemFound = true;
							break;
						}
					}
					//if no new prefix could be found, set pointer to -1
					if(!itemFound){
						prefixPointer[prefixPointerCounter] = -1;
					}else{
						prefixPointer[prefixPointerCounter]++; //set counter to next item
					}
				}
				prefixPointerCounter++;
			}
		}
		//debugging print out to terminal
		if(verbose > 1){
			System.out.println("new prefix pointer");
			for(int i = 0; i<prefixPointer.length; i++){
				System.out.println(i + " : " + prefixPointer[i]);
			}
			System.out.println();
		}
		return prefixPointer;
	}

	/**
	 * Getter of the determined frequent patterns.
	 * @return frequent patterns as map <frequent pattern, frequency>
	 */
	public HashMap<int[],Long> getFrequentPatterns(){
		return frequentPatterns;
	}
}