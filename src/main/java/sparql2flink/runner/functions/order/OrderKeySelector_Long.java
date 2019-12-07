package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.SolutionMappingHDT;
import sparql2flink.runner.functions.SolutionMappingURI;
import sparql2flink.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Long implements KeySelector<SolutionMappingHDT, Long> {

	static Dictionary dictionary;
	private String key;

	public OrderKeySelector_Long(Dictionary dictionary, String k) {
		this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Long getKey(SolutionMappingHDT sm) {
		return Long.parseLong(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}

}
