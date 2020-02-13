package sparql2flink.hdt.hdt.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.hdt.impl.HDTImpl;
import org.rdfhdt.hdt.header.HeaderPrivate;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TriplesPrivate;

/**
 * @author José M. Giménez-García
 *
 * @Note: HDTImpl modified to make fields protected instead of private
 *
 */
public class TransientHDT extends HDTImpl {

	public TransientHDT(HDTOptions spec) {
		super(spec);
	}

	public void setHeader(HeaderPrivate header) {
		this.setHeader(header);
	}

	public void setDictionary(DictionaryPrivate dictionary) {
		this.setDictionary(dictionary);
	}

	@Override
	public void setTriples(TriplesPrivate triples) {
		this.setTriples(triples);
	}

	@Override
	public void saveToHDT(OutputStream output, ProgressListener listener) throws IOException {
		// TODO Auto-generated method stub
		super.saveToHDT(output, listener);
	}

}
