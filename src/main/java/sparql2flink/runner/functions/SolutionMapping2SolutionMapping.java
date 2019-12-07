package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;

//SolutionMapping to SolutionMapping - Map Function
public class SolutionMapping2SolutionMapping implements MapFunction<SolutionMappingHDT, SolutionMappingHDT> {

    private String[] vars = null;

    public SolutionMapping2SolutionMapping(String[] vars){
        this.vars = vars;
    }

    @Override
    public SolutionMappingHDT map(SolutionMappingHDT sm){
        return sm.newSolutionMapping(vars);
    }
}

