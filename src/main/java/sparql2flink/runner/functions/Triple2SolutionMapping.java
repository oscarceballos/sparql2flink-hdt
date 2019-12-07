package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Triple;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.HashMap;

//Triple to SolutionMapping - Map Function
public class Triple2SolutionMapping implements MapFunction<TripleID, SolutionMappingHDT> {

    private String var_s, var_p, var_o = null;

    public Triple2SolutionMapping(String s, String p, String o){
        this.var_s = s;
        this.var_p = p;
        this.var_o = o;
    }

    @Override
    public SolutionMappingHDT map(TripleID t){
        SolutionMappingHDT sm = new SolutionMappingHDT();
        if(var_s!=null && var_p==null && var_o==null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
        } else if(var_s!=null && var_p!=null && var_o==null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
        } else if(var_s!=null && var_p==null && var_o!=null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else if(var_s==null && var_p!=null && var_o==null) {
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
        } else if(var_s==null && var_p!=null && var_o!=null) {
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else if(var_s==null && var_p==null && var_o!=null) {
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        }
        return sm;
    }

    /*@Override
    public SolutionMappingHDT map(TripleID t){
        SolutionMappingHDT sm = new SolutionMappingHDT();
        if(var_s!=null && var_p==null && var_o==null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
        } else if(var_s!=null && var_p!=null && var_o==null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
        } else if(var_s!=null && var_p==null && var_o!=null) {
            sm.putMapping(var_s, new Integer[]{t.getSubject(), 1});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else if(var_s==null && var_p!=null && var_o==null) {
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
        } else if(var_s==null && var_p!=null && var_o!=null) {
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else if(var_s==null && var_p==null && var_o!=null) {
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        } else {
            sm.putMapping(var_s, new Integer[]{t.getObject(), 3});
            sm.putMapping(var_p, new Integer[]{t.getPredicate(), 2});
            sm.putMapping(var_o, new Integer[]{t.getObject(), 3});
        }
        return sm;
    }*/
}
