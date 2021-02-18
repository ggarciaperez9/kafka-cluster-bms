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
        name = "emintec_patern_classify",
        description = "classify Emintec windows using algo_pattern AOI's data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmintecPaternClassifier {

    @Udf(description = "Apply classification model to algo_pattern table of AOI")
    public int emintec_patern_classify(
            @UdfParameter(value = "T_RangeAngle", description = "the value of T_RangeAngle") final double T_RangeAngle,
            @UdfParameter(value = "R_score", description = "the value of R_score") final double R_score,
            @UdfParameter(value = "R_angle", description = "the value of R_angle") final double R_angle,
            @UdfParameter(value = "R_offsetX", description = "the value of R_offsetX") final double R_offsetX,
            @UdfParameter(value = "R_offsetY", description = "the value of R_offsetY") final double R_offsetY) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_pattern/GBM_model_python_1600930320161_330.zip"));

        RowData row = new RowData();
        row.put("T_RangeAngle", T_RangeAngle);
        row.put("R_score", R_score);
        row.put("R_angle", R_angle);
        row.put("R_offsetX", R_offsetX);
        row.put("R_offsetY", R_offsetY);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
