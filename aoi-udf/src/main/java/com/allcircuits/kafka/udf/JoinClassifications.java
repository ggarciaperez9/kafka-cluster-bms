package com.allcircuits.kafka.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "join_classes",
        description = "Join the classifications of all algos",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class JoinClassifications {

    @Udf(description = "Join the classifications of all algos")
    public int join_classes(
            @UdfParameter(value = "bodyblob_prediction", description = "the value of bodyblob_prediction") final int bodyblob_prediction,
            @UdfParameter(value = "tilt_prediction", description = "the value of tilt_prediction") final int tilt_prediction,
            @UdfParameter(value = "heightmean_prediction", description = "the value of heightmean_prediction") final int heightmean_prediction,
            @UdfParameter(value = "leadsolder_prediction", description = "the value of leadsolder_prediction") final int leadsolder_prediction,
            @UdfParameter(value = "leadtip_prediction", description = "the value of leadtip_prediction") final int leadtip_prediction,
            @UdfParameter(value = "patern_prediction", description = "the value of patern_prediction") final int patern_prediction,
            @UdfParameter(value = "blackwhite_prediction", description = "the value of blackwhite_prediction") final int blackwhite_prediction,
            @UdfParameter(value = "graydiff_prediction", description = "the value of graydiff_prediction") final int graydiff_prediction,
            @UdfParameter(value = "heightdiff_prediction", description = "the value of heightdiff_prediction") final int heightdiff_prediction,
            @UdfParameter(value = "leadlift_prediction", description = "the value of leadlift_prediction") final int leadlift_prediction,
            @UdfParameter(value = "pocr_prediction", description = "the value of pocr_prediction") final int pocr_prediction,
            @UdfParameter(value = "soldercone_prediction", description = "the value of soldercone_prediction") final int soldercone_prediction) {

        if(tilt_prediction * bodyblob_prediction * heightmean_prediction * leadsolder_prediction * leadtip_prediction * patern_prediction * blackwhite_prediction * graydiff_prediction * heightdiff_prediction * leadlift_prediction * pocr_prediction * soldercone_prediction== 1) {
            return 0;
        }
        return 1;
    }
}
