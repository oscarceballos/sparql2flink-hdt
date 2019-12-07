package sparql2flink.runner.functions.filter;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.*;
import org.rdfhdt.hdt.dictionary.Dictionary;
import sparql2flink.runner.functions.TripleIDConvert;

import java.util.HashMap;

public class Equals{

	public static boolean eval(Dictionary dictionary, E_Equals expression, HashMap<String, Integer[]> sm){

		Expr arg1 = expression.getArg1();
		Expr arg2 = expression.getArg2();

		Boolean flag = false;
		Node value_left = null;
		Node value_right = null;

		if (arg1.isConstant() && arg2.isVariable()) {
			value_left = arg1.getConstant().getNode();
			value_right = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg2.toString()));
		} else if (arg1.isVariable() && arg2.isConstant()) {
			value_left = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg1.toString()));
			value_right = arg2.getConstant().getNode();
		} else if(arg1.isVariable() && arg2.isVariable()) {
			value_left = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg1.toString()));
			value_right = TripleIDConvert.idToStringFilter(dictionary, sm.get(arg2.toString()));
		}

        if(value_left.isLiteral()) {
            if (value_left.getLiteralValue().toString().equals(value_right.getLiteralValue().toString())) {
                //System.out.println("--- they are Equals ---");
                flag = true;
            }
        } else if(value_left.isURI()) {
            if (!(value_left.toString().equals(value_right.toString()))) {
                //System.out.println("--- they are Equals ---");
                flag = true;
            }
        }
        return flag;
	}
}
