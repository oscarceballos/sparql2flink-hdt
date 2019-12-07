package sparql2flink.runner.functions;

import org.apache.jena.graph.Node;
import java.util.HashMap;
import java.util.Map;

public class SolutionMappingURI {

	private HashMap<String, Node> mapping = new HashMap<>();

	public SolutionMappingURI() {}

	public void SolutionMappingURI(HashMap<String, Node> sm){
		this.mapping = sm;
	}

    public void setMapping(HashMap<String, Node> mapping){
        this.mapping = mapping;
    }

	public HashMap<String, Node> getMapping(){
		return mapping;
	}

	public void putMapping(String var, Node val) {
        mapping.put(var, val);
	}

	public Node getValue(String var){
		return mapping.get(var);
	}

	@Override
	public String toString() {
		String sm="";
		for (Map.Entry<String, Node> hm : mapping.entrySet()) {
		    if(hm.getValue() != null) {
                sm += hm.getKey() + "-->" + hm.getValue().toString() + "\t";
            }
		}
		return sm;
	}
}
