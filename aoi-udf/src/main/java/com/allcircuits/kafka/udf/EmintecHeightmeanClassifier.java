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
        name = "emintec_heightmean_classify",
        description = "classify Emintec windows using algo_heightmean AOI's data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmintecHeightmeanClassifier {

    @Udf(description = "Apply classification model to algo_heightmean table of AOI")
    public int emintec_heightmean_classify(
            @UdfParameter(value = "R_HeightMean", description = "the value of R_HeightMean") final double R_HeightMean) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_heightmean/GBM_model_python_1600930320161_464.zip"));

        RowData row = new RowData();
        row.put("R_HeightMean", R_HeightMean);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
