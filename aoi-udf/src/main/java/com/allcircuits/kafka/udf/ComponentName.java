package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.math.BigInteger;

@UdfDescription(
        name = "get_nameid",
        description = "Transform component name into BigInteger",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class ComponentName {

    @Udf(description = "Transform component name from string into BigInteger")
    public Long get_nameid(
            @UdfParameter(value = "component_name", description = "the value of component_name") final String component_name) {

        StringBuilder sb = new StringBuilder();
        for (char c : component_name.toCharArray())
            sb.append((int)c);

        return new BigInteger(sb.toString()).longValue();
    }
}
