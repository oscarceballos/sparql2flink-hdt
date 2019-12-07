package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.SolutionMappingHDT;
import sparql2flink.runner.functions.TripleIDConvert;


// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Float implements KeySelector<SolutionMappingHDT, Float> {

	static Dictionary dictionary;
	private String key;

	public OrderKeySelector_Float(Dictionary dictionary, String k) {
	    this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Float getKey(SolutionMappingHDT sm) {
		return Float.parseFloat(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}

}
