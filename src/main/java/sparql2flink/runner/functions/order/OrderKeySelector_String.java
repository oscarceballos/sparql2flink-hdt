package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.SolutionMappingHDT;
import sparql2flink.runner.functions.SolutionMappingURI;
import sparql2flink.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_String implements KeySelector<SolutionMappingHDT, String> {

    static Dictionary dictionary;
	private String key;

	public OrderKeySelector_String(Dictionary dictionary, String k) {
        this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public String getKey(SolutionMappingHDT sm) {
	    String value = "";
	    Node node = TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key));
		if(node.isLiteral()) {
            value = node.getLiteralValue().toString();
        }else if(node.isURI()){
            value = node.toString();
        }
        return value;
	}
}
