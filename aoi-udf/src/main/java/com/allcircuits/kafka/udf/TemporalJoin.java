package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "temp_join",
        description = "Join the classifications of all algos",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class TemporalJoin {

    @Udf(description = "Join the classifications of all algos")
    public int temp_join(
            @UdfParameter(value = "bodyblob_prediction", description = "the value of bodyblob_prediction") final int bodyblob_prediction,
            @UdfParameter(value = "heightmean_prediction", description = "the value of heightmean_prediction") final int heightmean_prediction,
            @UdfParameter(value = "blackwhite_prediction", description = "the value of blackwhite_prediction") final int blackwhite_prediction,
            @UdfParameter(value = "heightdiff_prediction", description = "the value of heightdiff_prediction") final int heightdiff_prediction) {

        return bodyblob_prediction * heightmean_prediction * blackwhite_prediction * heightdiff_prediction;
    }
}
