package prefixSpan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Input decoder for plain text files.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PlainInputDecoder implements InputDecoder{
	
	@Override
	public DataSet<Tuple2<Long,int[]>> parse(String inputFile, ExecutionEnvironment env) {
		return env.readTextFile(inputFile).map(new InputMapper());
	}

	private class InputMapper implements MapFunction<String,Tuple2<Long,int[]>>{
		private static final long serialVersionUID = -3804203011043200652L;
		@Override
		public Tuple2<Long, int[]> map(String rawInput) throws Exception {
			String[] rawInputItems = rawInput.split(",");
			int[] sequence = new int[rawInputItems.length-1];
			for(int i=1; i<rawInputItems.length; i++){
				sequence[i-1] = Integer.parseInt(rawInputItems[i]);
			}
			return new Tuple2<Long, int[]>(Long.parseLong(rawInputItems[0]),sequence);
		}
	}
}