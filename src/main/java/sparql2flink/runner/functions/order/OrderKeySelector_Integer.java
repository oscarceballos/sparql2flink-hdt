package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.SolutionMappingHDT;
import sparql2flink.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Integer implements KeySelector<SolutionMappingHDT, Integer> {

	static Dictionary dictionary;
	private String key;

	public OrderKeySelector_Integer(Dictionary dictionary, String k) {
		this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Integer getKey(SolutionMappingHDT sm) {
		return Integer.parseInt(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}

}
