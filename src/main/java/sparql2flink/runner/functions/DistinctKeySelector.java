package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;

// SolutionMapping - Distinct Key Selector
public class DistinctKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private String[] keys = null;

    public DistinctKeySelector(String[] keys){
        this.keys = keys;
    }

    @Override
    public String getKey(SolutionMappingHDT sm) {
        String value = "";
        int i=0;
        for (String key : keys) {
            if(sm.getMapping().containsKey(key)) {
                value += sm.getMapping().get(key)[0];
                if(++i < keys.length) {
                    value += ",";
                }
            }
        }
        return value;
    }
}
