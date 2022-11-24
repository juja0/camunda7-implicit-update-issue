package org.camunda.bpm.unittest;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.camunda.spin.impl.json.jackson.format.JacksonJsonDataFormat;
import org.camunda.spin.spi.DataFormatConfigurator;

public class JSR310Configurator implements DataFormatConfigurator<JacksonJsonDataFormat>
{
	@Override
	public Class<JacksonJsonDataFormat> getDataFormatClass()
	{
		return JacksonJsonDataFormat.class;
	}

	@Override
	public void configure(JacksonJsonDataFormat dataFormat)
	{
		dataFormat.getObjectMapper().registerModule(new JavaTimeModule());
	}
}
