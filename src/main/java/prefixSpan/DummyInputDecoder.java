package prefixSpan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Input decoder which stores three sample databases for manual testing.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class DummyInputDecoder implements InputDecoder {

	@Override
	public DataSet<Tuple2<Long,int[]>> parse(String inputFile, ExecutionEnvironment env) {
		
		DataSet<Tuple2<Long,int[]>> data;
		
		//first sample database for testing
		if(inputFile != null && inputFile.equals("1")){
			Tuple2<Long,int[]> a = new Tuple2<Long,int[]>(0L,new int[]{0,1,2,3,4,0});					//(abcd)
			Tuple2<Long,int[]> b = new Tuple2<Long,int[]>(1L,new int[]{0,1,2,4,0});						//(abd)
			data = env.fromElements(a,b);
		}
		
		//second sample database for testing
		else if(inputFile != null && inputFile.equals("2")){
			Tuple2<Long,int[]> a = new Tuple2<Long,int[]>(0L,new int[]{0,1,0,1,2,3,0,4,0});				//a(abc)d
			Tuple2<Long,int[]> b = new Tuple2<Long,int[]>(1L,new int[]{0,4,5,0,1,3,0,2,6,0});			//(de)(ac)(bf)
			data = env.fromElements(a,b);
		}
		
		//default sample database from Pei et al., Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach
		else{
			Tuple2<Long,int[]> a = new Tuple2<Long,int[]>(0L,new int[]{0,1,0,1,2,3,0,1,3,0,4,0,3,6,0});	//a(abc)(ac)d(cf)
			Tuple2<Long,int[]> b = new Tuple2<Long,int[]>(1L,new int[]{0,1,4,0,3,0,2,3,0,1,5,0});		//(ad)c(bc)(ae)
			Tuple2<Long,int[]> c = new Tuple2<Long,int[]>(2L,new int[]{0,5,6,0,1,2,0,4,6,0,3,0,2,0});	//(ef)(ab)(df)cb
			Tuple2<Long,int[]> d = new Tuple2<Long,int[]>(3L,new int[]{0,5,0,7,0,1,6,0,3,0,2,0,3,0});	//eg(af)cbc
			data = env.fromElements(a,b,c,d);
		}
		
		return data;
	}
}
