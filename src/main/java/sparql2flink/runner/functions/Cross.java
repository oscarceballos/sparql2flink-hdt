package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.CrossFunction;

//SolutionMapping - Cross Function
public class Cross implements CrossFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public SolutionMappingHDT cross(SolutionMappingHDT left, SolutionMappingHDT right) {
        return left.join(right);
    }
}
