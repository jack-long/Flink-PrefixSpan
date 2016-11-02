package prefixSpan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public interface InputDecoder {

	/**
	 * Parses the input file type for prefixSpan usage. 
	 * @param inputFile the file to parse
	 * @param env the flink ExecutionEnvironment to use
	 * @return parsed database
	 */
	DataSet<Tuple2<Long,int[]>> parse(String inputFile, ExecutionEnvironment env) throws Exception;
	
}
