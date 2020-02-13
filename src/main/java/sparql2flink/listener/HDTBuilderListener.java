package sparql2flink.listener;

import org.rdfhdt.hdt.listener.ProgressListener;
import sparql2flink.mrbuilder.HDTBuilderConfiguration;

public class HDTBuilderListener implements ProgressListener {

	boolean	quiet;

	public HDTBuilderListener(HDTBuilderConfiguration conf) {
		this.quiet = conf.getQuiet();
	}

	@Override
	public void notifyProgress(float level, String message) {
		if (!this.quiet) {
			System.out.print("\r" + message + "\t" + Float.toString(level) + "                            \r");
		}
	}
}