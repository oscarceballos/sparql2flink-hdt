package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.util.HashMap;
import java.util.Map;

//SolutionMapping to SolutionMapping - Map Function
public class TripleID2TripleString implements MapFunction<SolutionMappingHDT, SolutionMappingURI> {

    private static Dictionary dictionary;

    public TripleID2TripleString(Dictionary d){
        dictionary = d;
    }

    @Override
    public SolutionMappingURI map(SolutionMappingHDT smHDT) {
        SolutionMappingURI smURI = new SolutionMappingURI();
        for (Map.Entry<String, Integer[]> hm : smHDT.getMapping().entrySet()) {
            smURI.putMapping(hm.getKey(), TripleIDConvert.idToString(dictionary, hm.getValue()));
        }
        return smURI;
    }
}

