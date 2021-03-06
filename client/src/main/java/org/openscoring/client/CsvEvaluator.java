/*
 * Copyright (c) 2014 Villu Ruusmann
 *
 * This file is part of Openscoring
 *
 * Openscoring is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Openscoring is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Openscoring.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.openscoring.client;

import java.io.*;

import javax.ws.rs.core.*;

import com.sun.jersey.api.client.*;

import com.beust.jcommander.*;

public class CsvEvaluator extends Application {

	@Parameter (
		names = {"--model"},
		description = "The URI of the model",
		required = true
	)
	private String model = null;

	@Parameter (
		names = {"--input"},
		description = "Input CSV file",
		required = true
	)
	private File input = null;

	@Parameter (
		names = {"--output"},
		description = "Output CSV file",
		required = true
	)
	private File output = null;

	@Parameter (
		names = {"--id-column"},
		description = "The name of the row identifier column"
	)
	private String idColumn = null;


	static
	public void main(String... args) throws Exception {
		run(CsvEvaluator.class, args);
	}

	@Override
	public void run() throws IOException {
		Client client = Client.create();

		WebResource resource = client.resource(ensureSuffix(this.model, "/csv"));
		if(this.idColumn != null){
			resource = resource.queryParam("idColumn", this.idColumn);
		}

		InputStream is = new FileInputStream(this.input);

		try {
			OutputStream os = new FileOutputStream(this.output);

			try {
				InputStream result = resource.type(MediaType.TEXT_PLAIN).post(InputStream.class, is);

				try {
					copy(result, os);
				} finally {
					result.close();
				}
			} finally {
				os.close();
			}
		} finally {
			is.close();
		}

		client.destroy();
	}

	static
	private String ensureSuffix(String string, String suffix){

		if(!string.endsWith(suffix)){
			string += suffix;
		}

		return string;
	}

	static
	private void copy(InputStream is, OutputStream os) throws IOException {
		byte[] buffer = new byte[512];

		while(true){
			int count = is.read(buffer);
			if(count < 0){
				break;
			}

			os.write(buffer, 0, count);
		}

		os.flush();
	}
}