package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;

public class CoGroupKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private String[] keys = null;

    public CoGroupKeySelector(String[] keys){
        this.keys = keys;
    }

    @Override
    public String getKey(SolutionMappingHDT sm) {
        String value = "";
        int i=0;
        for (String key : keys) {
            value += sm.getMapping().get(key)[0];
            if(++i < keys.length) {
                value += ",";
            }
        }
        return value;
    }

}
