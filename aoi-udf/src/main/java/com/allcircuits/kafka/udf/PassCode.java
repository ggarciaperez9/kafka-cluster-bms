package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "as_pass_code",
        description = "Change predictions code to pass code",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class PassCode {

    @Udf(description = "Change predictions code to pass code")
    public int join_classes(
            @UdfParameter(value = "algo_prediction", description = "the value of algo_prediction") final int algo_prediction) {

        if(algo_prediction == 1) {
            return 0;
        }
        return 1;
    }
}
