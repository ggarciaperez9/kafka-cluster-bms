package com.allcircuits.kafka.udf;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.io.IOException;

@UdfDescription(
        name = "parse_historic",
        description = "Parse the historic file of the AOI",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class ParserHistoric {
    @Udf(description = "Apply classification model to algo_pattern table of AOI")
    public int parse_historic(
            @UdfParameter(value = "raw_text", description = "the value of raw_text") final String raw_text) throws PredictException, IOException {

            return 1;
    }
}
