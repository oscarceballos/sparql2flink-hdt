package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Union implements CoGroupFunction<SolutionMappingHDT, SolutionMappingHDT, SolutionMappingHDT> {

    @Override
    public void coGroup(Iterable<SolutionMappingHDT> left, Iterable<SolutionMappingHDT> right, Collector<SolutionMappingHDT> out) throws Exception {

        Set<SolutionMappingHDT> smSet = new HashSet<>();

        for (SolutionMappingHDT smLeft : left) {
            smSet.add(smLeft);
        }

        for (SolutionMappingHDT smRight : right) {
            smSet.add(smRight);
        }

        for (SolutionMappingHDT sm : smSet) {
            out.collect(sm);
        }
    }

}
