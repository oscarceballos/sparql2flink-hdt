package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;

// SolutionMapping - Key Selector Join
public class JoinKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private String[] keys;

    public JoinKeySelector(String[] keys){
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
