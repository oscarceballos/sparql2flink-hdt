package sparql2flink.runner.functions;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

public class TripleIDConvert {

    public static Node idToString(Dictionary dictionary, Integer[] id) {
        Node element;
        if (id[1] == 1) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.SUBJECT).toString());
        } else if (id[1] == 2) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.PREDICATE).toString());
        } else {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.OBJECT).toString());
        }
        return element;
    }

    public static Node idToStringFilter(Dictionary dictionary, Integer[] id) {
        Node element = null;
        if (id[1] == 1) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.SUBJECT).toString());
        } else if (id[1] == 2) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.PREDICATE).toString());
        } else {
            String object = dictionary.idToString(id[0], TripleComponentRole.OBJECT).toString();
            if(object.contains("^^")){
                int start = 1, end = object.indexOf("^")-1, hashtag = object.indexOf("#")+1;
                switch (object.substring(hashtag, object.length()-1)) {
                    case "integer":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDint);
                        break;
                    case "boolean":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDboolean);
                        break;
                    case "dateTime":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdateTime);
                        break;
                    case "date":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdate);
                        break;
                    case "decimal":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdecimal);
                        break;
                    case "double":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdouble);
                        break;
                    case "byte":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDbyte);
                        break;
                    case "float":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDfloat);
                        break;
                    case "long":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDlong);
                        break;
                    case "string":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDstring);
                        break;
                }
            } else {
                element = NodeFactory.createURI(object.toString());
            }
        }
        return element;
    }

    public static Integer stringToIDSubject(Dictionary dictionary, String element) {
        return dictionary.stringToId(element, TripleComponentRole.SUBJECT);
    }

    public static Integer stringToIDPredicate(Dictionary dictionary, String element) {
        return dictionary.stringToId(element, TripleComponentRole.PREDICATE);
    }

    public static Integer stringToIDObject(Dictionary dictionary, String element) {
        return dictionary.stringToId(element, TripleComponentRole.OBJECT);
    }
}
