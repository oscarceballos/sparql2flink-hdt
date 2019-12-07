package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.SolutionMappingHDT;
import sparql2flink.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Double implements KeySelector<SolutionMappingHDT, Double> {

	static Dictionary dictionary;
	private String key;

	public OrderKeySelector_Double(Dictionary dictionary, String k) {
		this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Double getKey(SolutionMappingHDT sm) {
		return Double.parseDouble(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}

}
