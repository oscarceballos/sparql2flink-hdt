package sparql2flink.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path){
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = path.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1, this.className.length());
    }

    public String logicalQueryPlan2FlinkProgram() {
        String flinkProgram = "";

        flinkProgram += "package sparql2flink.out;\n\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.common.operators.Order;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.jena.graph.Node;\n" +

                "import org.rdfhdt.hdt.hdt.HDT;\n" +
                "import org.rdfhdt.hdt.triples.IteratorTripleID;\n" +
                "import org.rdfhdt.hdt.triples.TripleID;\n" +

                "import sparql2flink.runner.functions.*;\n" +
                "import sparql2flink.runner.LoadTriples;\n" +
                "import sparql2flink.runner.functions.order.*;\n" +
                "import java.math.*;\n" +
                "import java.util.ArrayList;\n" +

                "\npublic class "+className+" {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +
                "\t\t}\n\n" +
                "\t\t//************ Environment (DataSet) and Source (static RDF dataset) ************\n" +
                "\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tHDT hdt = LoadTriples.fromDataset(env, params.get(\"dataset\"));\n\n" +

                "\t\tArrayList<TripleID> listTripleID = new ArrayList<>();\n" +
                "\t\tIteratorTripleID iterator = hdt.getTriples().searchAll();\n" +
                "\t\twhile(iterator.hasNext()) {\n" +
                "\t\t\tTripleID tripleID = new TripleID(iterator.next());\n" +
                "\t\t\tlistTripleID.add(tripleID);\n" +
                "\t\t}\n\n" +

                "\t\tDataSet<TripleID> dataset = env.fromCollection(listTripleID);\n\n" +

                "\t\t//************ Applying Transformations ************\n";

        logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());

        flinkProgram += ConvertLQP2FlinkProgram.getFlinkProgram();

        flinkProgram += "\t\tDataSet<SolutionMappingURI> sm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.map(new TripleID2TripleString(hdt.getDictionary()));\n\n";

        flinkProgram += "\t\t//************ Sink  ************\n" +
                "\t\tsm"+(SolutionMapping.getIndice()) +
                ".writeAsText(params.get(\"output\")+\""+className+"-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +
                "\t\tenv.execute(\"SPARQL Query to Flink Programan - DataSet API\");\n" +
                "\t}\n}";

        return flinkProgram;
    }
}
