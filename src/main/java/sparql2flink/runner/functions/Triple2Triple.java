package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.triples.TripleID;

//Triple to Triple - Filter Function
public class Triple2Triple implements FilterFunction<TripleID> {

    private static Dictionary dictionary = null;
    private String subject, predicate, object = null;

    public Triple2Triple(Dictionary d, String s, String p, String o){
        this.dictionary = d;
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    @Override
    public boolean filter(TripleID t) {
        Integer s, p, o = null;
        if(subject==null && predicate!=null && object!=null) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return (t.getPredicate() == p && t.getObject() == o);
        } else if(subject!=null && predicate==null && object!=null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return (t.getSubject() == s && t.getObject() == o);
        } else if(subject!=null && predicate!=null && object==null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            return (t.getSubject() == s && t.getPredicate() == p);
        } else if(subject!=null && predicate==null && object==null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            return t.getSubject() == s;
        } else if(subject==null && predicate!=null && object==null) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            return t.getPredicate() == p;
        } else if(subject==null && predicate==null && object!=null) {
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return t.getObject() == o;
        } else {
            return true;
        }
    }
}
