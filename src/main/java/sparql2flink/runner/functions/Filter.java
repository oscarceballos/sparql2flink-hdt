package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.dictionary.Dictionary;

//SolutionMapping to SolutionMapping - Filter Function
public class Filter implements FilterFunction<SolutionMappingHDT> {

    private static Dictionary dictionary = null;
    private String expression = null;

    public Filter(Dictionary dictionary, String expression){
        this.dictionary = dictionary;
        this.expression = expression;
    }

    @Override
    public boolean filter(SolutionMappingHDT sm) {
        return sm.filter(dictionary, expression);
    }
}
