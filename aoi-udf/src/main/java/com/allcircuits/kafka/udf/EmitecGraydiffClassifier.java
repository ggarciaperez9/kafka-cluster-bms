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
        name = "emitec_graydiff_classify",
        description = "classify Emitec windows using algo_graydiff AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecGraydiffClassifier {

    @Udf(description = "Apply classification model to algo_graydiff table of AOI")
    public int emitec_graydiff_classify(
            @UdfParameter(value = "T_GrayDiff", description = "the value of T_GrayDiff") final double T_GrayDiff,
            @UdfParameter(value = "R_GrayDiff", description = "the value of R_GrayDiff") final double R_GrayDiff) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_graydiff/GBM_model_python_1600930320161_695.zip"));

        RowData row = new RowData();
        row.put("T_GrayDiff", T_GrayDiff);
        row.put("R_GrayDiff", R_GrayDiff);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
