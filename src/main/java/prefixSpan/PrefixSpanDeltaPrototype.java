package prefixSpan;

/**
 * Created by jack on 1/18/16.
 */
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class PrefixSpanDeltaPrototype {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        DataSet<String> searchSpace =
//                env.fromCollection(Lists.newArrayList("foo", "bar", "foobar"));
//
        // get input data
        DataSet<String> input_data = env.fromElements(
                "a cba ac d cf",
                "ad c bc ae",
                "ef ab df c b",
                "e g af c b c"
        );

        // fix the order in each itemset
        DataSet<String> searchSpace = input_data.map(new MapFunction<String, String>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 4484271106788054779L;

			@Override
            public String map(String sequence){
                String sorted_itemset = "";
                for(String itemset : sequence.split(" ")){
                    char[] items = itemset.toCharArray();
                    Arrays.sort(items);
                    sorted_itemset = sorted_itemset + " " + new String(items); // leading ' ' should be removed later
                }
                return new String(sorted_itemset.substring(1));
            }
        });

        // expand size 1 patterns (initial workset)
        DataSet<Tuple4<String,String, Integer, Integer>> embeddings = searchSpace
                .flatMap(new SizeOneEmbeddingExpander());
//                .sortPartition(1, Order.ASCENDING);

        embeddings.print();  // print the sorted sequences

        // empty result collection (initial solution set)
		@SuppressWarnings("unchecked")
		DataSet<Tuple1<String>> allFrequentPatterns =
                env.fromCollection(Lists.newArrayList(new Tuple1<>("")))
                        .filter(new AlwaysFalseFilter());

        // while embeddings not empty
        DeltaIteration<Tuple1<String>, Tuple4<String, String, Integer, Integer>>
                whileNotEmpty = allFrequentPatterns
                .iterateDelta(embeddings, 10, 0);  // change this 30 smaller, for example, 2, to generate length-2 patterns.

        // find frequent patterns of current pattern length
        // get new solutions
        // collect frequent patterns from candidate patterns.
        DataSet<Tuple1<String>> frequentPatterns = whileNotEmpty.getWorkset()
                .groupBy(0, 1).min(3) // distinct report per item and pattern
                .groupBy(1).sum(3) // count support
                .filter(new MinSupportFilter())  // {return f.3 >= 2}
                .map(new PatternOnly());  // {return f.1}

        // grow embeddings of frequent patterns for next iteration
        DataSet<Tuple4<String,String, Integer, Integer>> grownEmbeddings =
                whileNotEmpty.getWorkset()
                        .join(frequentPatterns)
                        .where(1).equalTo(0)
                        .with(new LeftSideOnly())  // only return the left side table; filter frequent ones
                        .flatMap(new PatternGrower());

        // add frequentPatterns to solution while grownEmbeddings not empty
        allFrequentPatterns = whileNotEmpty
                .closeWith(frequentPatterns, grownEmbeddings);

        allFrequentPatterns.print(); //.sortLocalOutput(1, Order.ASCENDING);

    }

    static class SizeOneEmbeddingExpander
            implements FlatMapFunction<String, Tuple4<String,String, Integer, Integer>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -6404623488946653796L;

		@Override
        public void flatMap(String s,
                            Collector<Tuple4<String,String, Integer, Integer>> collector) throws Exception {
            int index = 0;  // create index for subfix, points to the start.
            for(char c : s.toCharArray()) {
                if(c != ' '){
                    collector.collect(new Tuple4<>(s, String.valueOf(c), index + 1, 1));
                }
                index++;
            }
        }
    }

    private static class AlwaysFalseFilter
            implements FilterFunction<Tuple1<String>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 7730627196927623285L;

		@Override
        public boolean filter(Tuple1<String> t) throws Exception {
            return false;
        }
    }

    private static class MinSupportFilter
            implements FilterFunction<Tuple4<String,String, Integer, Integer>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -6422226721458121326L;

		@Override
        public boolean filter(
                Tuple4<String,String, Integer, Integer> triple) throws Exception {
        	System.out.println(triple.f1 + " " + triple.f3);
        	return triple.f3 >= 2;
        }
    }

    private static class PatternOnly
            implements MapFunction<Tuple4<String,String, Integer, Integer>, Tuple1<String>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 7063130335060709977L;

		@Override
        public Tuple1<String> map(
                Tuple4<String,String, Integer, Integer> triple) throws Exception {
            return new Tuple1<>(triple.f1);
        }
    }

    private static class LeftSideOnly
            implements JoinFunction<Tuple4<String,String, Integer, Integer>, Tuple1<String>,
            Tuple4<String,String, Integer, Integer>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -2919530016870920001L;

		@Override
        public Tuple4<String,String, Integer, Integer> join(
                Tuple4<String,String, Integer, Integer> embedding,
                Tuple1<String> pattern) throws Exception {
            return embedding;
        }
    }

    private static class PatternGrower implements FlatMapFunction
            <Tuple4<String,String, Integer, Integer>, Tuple4<String,String, Integer, Integer>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -4881153187733246045L;

		@Override
        public void flatMap(Tuple4<String,String, Integer, Integer> embedding,
                            Collector<Tuple4<String,String, Integer, Integer>> collector) throws
                Exception {
            String sequence = embedding.f0;
            String pattern = embedding.f1;

            int subfix_pointer = embedding.f2;
//
//            List<Integer> indexs = getIndexes(sequence, pattern);
//
//            if(pattern.equals("a") && sequence.equals("a abc ac d cf")) {
//                System.out.println("-- Debug --: Size of index " + indexs.size());
//            }
//
            String prfix = "";

//            if(pattern.equals("a") && sequence.equals("a abc ac d cf")){
//                System.out.println("-- Debug --:" + subfix_pointer);
//            }
            // boundary test
            if(subfix_pointer <= sequence.length()-1){
                String subfix = sequence.substring(subfix_pointer);

//                if(pattern.equals("a") && sequence.equals("a abc ac d cf")){
//                    System.out.println("-- Debug --:" + sequence + ", " + subfix);
//                }

                if(subfix.charAt(0) == ' '){  // subfix: " abc ef ac"

                    // System.out.println("-- Debug --:" + sequence + ", pattern:" + pattern);

                    prfix = pattern + ' ';

                    int pointer = subfix_pointer;

                    for(char sc : subfix.toCharArray()){
                        pointer++;
                        if(sc != ' '){
                            String grownPattern = prfix + sc;
                            collector.collect(
                                    new Tuple4<>(sequence, grownPattern, pointer, 1));
                        }

                    }
                }
                else {

                    int space_index = subfix.indexOf(' ');

                    if(space_index == -1){ // subfix: "bcfg"
                        int pointer = subfix_pointer;

                        for(char sc : subfix.toCharArray()){
                            pointer++;
                            String grownPattern = pattern + sc;
                            collector.collect(
                                    new Tuple4<>(sequence, grownPattern, pointer, 1));
                        }
                    }
                    else {  // subfix: "bc f g"
                        int pointer = subfix_pointer;

                        for(char sc : subfix.substring(0, space_index).toCharArray()){
                            pointer++;
                            String grownPattern = pattern + sc;
                            collector.collect(
                                    new Tuple4<>(sequence, grownPattern, pointer, 1));
                        }

//                        if(pattern.equals("a")){
//                            System.out.println("-- Debug --:" + sequence + ", First:" + subfix.substring(0, boundary_index)
//                                    + ", Second:" + subfix.substring(boundary_index + 1));
//                        }

                        subfix = subfix.substring(space_index + 1);
                        pointer++;

                        prfix = pattern + ' ';
                        for(char sc : subfix.toCharArray()){
                            pointer++;
                            if(sc != ' '){
                                String grownPattern = prfix + sc;
                                collector.collect(
                                        new Tuple4<>(sequence, grownPattern, pointer, 1));
                            }
                        }
                    }

                }

            }



        }

    }
}