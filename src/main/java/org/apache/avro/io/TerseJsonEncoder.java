package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/**
 * {@link JsonEncoder} that doesn't use any separator between top-level objects.
 */

public class TerseJsonEncoder extends JsonEncoder {

	public TerseJsonEncoder(Schema sc, OutputStream out) throws IOException {
		super(sc, jsonGenerator(out));
	}

	private static JsonGenerator jsonGenerator(OutputStream out) throws IOException {
		JsonGenerator g = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
		MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
		pp.setRootValueSeparator("");
		g.setPrettyPrinter(pp);
		return g;
	}
}
