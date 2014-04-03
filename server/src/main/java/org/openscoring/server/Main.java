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
package org.openscoring.server;

import java.util.*;
import java.util.concurrent.*;

import java.io.*;

import javax.servlet.*;

import org.openscoring.service.*;

import com.google.common.collect.*;
import com.google.inject.*;
import com.google.inject.servlet.*;

import com.sun.jersey.api.json.*;
import com.sun.jersey.guice.*;
import com.sun.jersey.guice.spi.container.servlet.*;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.*;
import org.eclipse.jetty.util.thread.*;

import com.beust.jcommander.*;

import com.codahale.metrics.*;
import com.codahale.metrics.jersey.*;
import com.codahale.metrics.jetty9.*;

public class Main {

	@Parameter (
		names = {"--port"},
		description = "Port"
	)
	private int port = 8080;

	@Parameter (
		names = {"--context-path"},
		description = "Context path"
	)
	private String contextPath = "/openscoring";

	@Parameter (
		names = {"--help"},
		description = "Show the list of configuration options and exit",
		help = true
	)
	private boolean help = false;

	@Parameter (
		names = {"--max-threads"},
		description = "The maximum number of threads in the threadpool."
	)
	private int maxThreads = 1;

	@Parameter (
		names = {"--min-threads"},
		description = "The minimum number of threads in the threadpool."
	)
	private int minThreads = 1;

	@Parameter (
		names = {"--metrics-dir"},
		description = "The directory where metrics are stored."
	)
	private String metricsDir = "";

	static
	public void main(String... args) throws Exception {
		Main main = new Main();

		JCommander commander = new JCommander(main);
		commander.setProgramName(Main.class.getName());

		try {
			commander.parse(args);
		} catch(ParameterException pe){
			commander.usage();

			System.exit(-1);
		}

		if(main.help){
			commander.usage();

			System.exit(0);
		}

		main.run();
	}

	private void run() throws Exception {
		final MetricRegistry metrics = new MetricRegistry();

		InstrumentedQueuedThreadPool threadPool = new InstrumentedQueuedThreadPool(metrics, this.maxThreads, this.minThreads);
		Server server = new Server(threadPool);

		ServerConnector connector = new ServerConnector(server);
		connector.setPort(this.port);
		server.setConnectors(new Connector[] { connector });

		ServletContextHandler contextHandler = new ServletContextHandler();
		contextHandler.setContextPath(this.contextPath);

		contextHandler.addFilter(GuiceFilter.class, "/*", null);

		Module module = new JerseyServletModule(){

			@Override
			public void configureServlets(){
				bind(ModelService.class);
				bind(InstrumentedResourceMethodDispatchAdapter.class)
				  .toInstance(new InstrumentedResourceMethodDispatchAdapter(metrics));

				Map<String, String> config = Maps.newLinkedHashMap();
				config.put(JSONConfiguration.FEATURE_POJO_MAPPING, "true");

				serve("/*").with(GuiceContainer.class, config);
			}
		};

		final
		Injector injector = Guice.createInjector(module);

		ServletContextListener contextListener = new GuiceServletContextListener(){

			@Override
			protected Injector getInjector(){
				return injector;
			}
		};
		contextHandler.addEventListener(contextListener);

		contextHandler.addServlet(DefaultServlet.class, "/");

		server.setHandler(contextHandler);

		server.start();

		if (!this.metricsDir.trim().equals("")) {
			System.out.printf("Reporting metrics in %s\n", this.metricsDir);
			CsvReporter reporter = CsvReporter.forRegistry(metrics)
			                                  .formatFor(Locale.US)
			                                  .convertRatesTo(TimeUnit.SECONDS)
			                                  .convertDurationsTo(TimeUnit.MILLISECONDS)
			                                  .build(new File(this.metricsDir));
			reporter.start(1, TimeUnit.SECONDS);
			// JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
			// reporter.start();
		} else {
			System.out.println("No metrics will be reported.");
		}

		server.join();
	}
}
