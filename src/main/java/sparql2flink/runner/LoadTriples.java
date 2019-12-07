package sparql2flink.runner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;


public class LoadTriples {
	public static HDT fromDataset(ExecutionEnvironment environment, String filePath) {
	    Preconditions.checkNotNull(filePath, "The file path may not be null...");
        HDT hdt = null;
        try {
            hdt = HDTManager.generateHDT(filePath, "", RDFNotation.parse("ntriples"), new HDTSpecification(),null);
        }catch (Exception e){

        }
        return hdt;
	}
}


