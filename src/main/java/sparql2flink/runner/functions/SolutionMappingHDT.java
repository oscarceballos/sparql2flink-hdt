package sparql2flink.runner.functions;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.sse.SSE;

import java.util.HashMap;
import java.util.Map;

public class SolutionMappingHDT {

    private HashMap<String, Integer[]> mapping = new HashMap<>();

	public SolutionMappingHDT() {}

	public void SolutionMappingHDT(HashMap<String, Integer[]> sm){
		this.mapping = sm;
	}

    public void setMapping(HashMap<String, Integer[]> mapping){
        this.mapping = mapping;
    }

	public HashMap<String, Integer[]> getMapping(){
		return mapping;
	}

	public void putMapping(String var, Integer[] val) {
        mapping.put(var, val);
	}

	public Integer[] getValue(String var){
		return mapping.get(var);
	}

	public boolean existMapping(String var, Integer val){
		Boolean flag = false;
        if(mapping.containsKey(var)) {
            if(mapping.get(var)[0] == val) {
                flag = true;
            }
        }
		return flag;
	}

	public SolutionMappingHDT join(SolutionMappingHDT sm){
        for (Map.Entry<String, Integer[]> hm : sm.getMapping().entrySet()) {
            if (!existMapping(hm.getKey(), hm.getValue()[0])) {
                this.putMapping(hm.getKey(), hm.getValue());
            }
        }
		return this;
	}

    public SolutionMappingHDT leftJoin(SolutionMappingHDT sm) {
        if(sm != null) {
            for (Map.Entry<String, Integer[]> hm : sm.getMapping().entrySet()) {
                if (!existMapping(hm.getKey(), hm.getValue()[0])) {
                    this.putMapping(hm.getKey(), hm.getValue());
                }
            }
        }
        return this;
    }

	public SolutionMappingHDT newSolutionMapping(String[] vars){
		SolutionMappingHDT sm = new SolutionMappingHDT();
		for (String var : vars) {
            if(var != null) {
                sm.putMapping(var, mapping.get(var));
            }
		}
		return sm;
	}

    public SolutionMappingHDT project(String[] vars){
        SolutionMappingHDT sm = new SolutionMappingHDT();
        for (String var : vars) {
            if(mapping.containsKey(var)) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
    }

	public boolean filter(Dictionary dictionary, String expression){
		Expr expr = SSE.parseExpr(expression);
		return FilterConvert.convert(dictionary, expr, this.getMapping());
	}


	@Override
	public String toString() {
		String sm="";
		for (Map.Entry<String, Integer[]> hm : mapping.entrySet()) {
		    if(hm.getValue() != null) {
                sm += hm.getKey() + "-->" + hm.getValue()[0] + "\t";
            }
		}
		return sm;
	}
}
