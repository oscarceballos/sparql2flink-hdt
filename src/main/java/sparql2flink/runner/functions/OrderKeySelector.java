package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import org.rdfhdt.hdt.dictionary.Dictionary;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector implements KeySelector<SolutionMappingHDT, String> {

    private static Dictionary dictionary = null;
	private String key;

	public OrderKeySelector(Dictionary dictionary, String key) {
	    this.dictionary = dictionary;
		this.key = key;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
        String value = "";
	    Node node = TripleIDConvert.idToString(dictionary, sm.getMapping().get(key));
        if(node.isLiteral()) {
            value = node.getLiteralValue().toString();
        }else if(node.isURI()){
            value = sm.getMapping().get(key).toString();
        }
        return value;
	}
}
