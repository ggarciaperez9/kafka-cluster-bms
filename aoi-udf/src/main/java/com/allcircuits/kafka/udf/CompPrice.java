package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "get_price",
        description = "Get Price of components",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class CompPrice {

    @Udf(description = "Divide the price by 1000")
    public double get_price(
            @UdfParameter(value = "PRIX_STANDARD", description = "the value of PRIX_STANDARD") final double PRIX_STANDARD) {

        return PRIX_STANDARD * 0.001;
    }
}