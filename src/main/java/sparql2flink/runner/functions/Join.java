package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

//SolutionMapping - Flat Join Function
public class Join implements FlatJoinFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public void join(SolutionMappingHDT left, SolutionMappingHDT right, Collector<SolutionMappingHDT> out) throws Exception {
        out.collect(left.join(right));
    }
}
