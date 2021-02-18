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
        name = "emitec_pocr_classify",
        description = "classify Emintec windows using algo_pocr AOI's data",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecPocrClassifier {

    @Udf(description = "Apply classification model to algo_pocr table of AOI")
    public int emitec_pocr_classify(
            @UdfParameter(value = "T_TargetFont", description = "the value of T_TargetFont") final String T_TargetFont,
            @UdfParameter(value = "T_Score", description = "the value of T_Score") final double T_Score,
            @UdfParameter(value = "R_Score", description = "the value of R_Score") final double R_Score) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_pocr/GBM_model_python_1600930320161_567.zip"));

        RowData row = new RowData();
        row.put("T_TargetFont", T_TargetFont);
        row.put("T_Score", T_Score);
        row.put("R_Score", R_Score);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
