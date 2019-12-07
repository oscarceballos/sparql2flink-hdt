package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;

//SolutionMapping to SolutionMapping - Map Function
public class Project implements MapFunction<SolutionMappingHDT, SolutionMappingHDT> {

    private String[] vars = null;

    public Project(String[] vars){
        this.vars = vars;
    }

    @Override
    public SolutionMappingHDT map(SolutionMappingHDT sm){
        return sm.project(vars);
    }
}

