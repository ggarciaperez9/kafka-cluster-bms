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
        name = "emitec_tilt_classify",
        description = "classify Emitec windows using algo_tilt AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecTiltClassifier {

    @Udf(description = "Apply classification model to algo_tilt table of AOI")
    public int emitec_tilt_classify(
            @UdfParameter(value = "T_AllowDiff", description = "the value of T_AllowDiff") final double T_AllowDiff,
            @UdfParameter(value = "R_HeightDiff", description = "the value of R_HeightDiff") final double R_HeightDiff,
            @UdfParameter(value = "R_ArrRectHeight1", description = "the value of R_ArrRectHeight1") final double R_ArrRectHeight1,
            @UdfParameter(value = "R_ArrRectHeight2", description = "the value of R_ArrRectHeight2") final double R_ArrRectHeight2,
            @UdfParameter(value = "R_ArrRectHeight3", description = "the value of R_ArrRectHeight3") final double R_ArrRectHeight3,
            @UdfParameter(value = "R_ArrRectHeight4", description = "the value of R_ArrRectHeight4") final double R_ArrRectHeight4,
            @UdfParameter(value = "R_HeightDiffMin", description = "the value of R_HeightDiffMin") final double R_HeightDiffMin,
            @UdfParameter(value = "R_HeightDiffMax", description = "the value of R_HeightDiffMax") final double R_HeightDiffMax,
            @UdfParameter(value = "T_Height", description = "the value of T_Height") final double T_Height,
            @UdfParameter(value = "T_HeightMin", description = "the value of T_HeightMin") final double T_HeightMin,
            @UdfParameter(value = "T_HeightMax", description = "the value of T_HeightMax") final double T_HeightMax,
            @UdfParameter(value = "R_AllAngles", description = "the value of R_AllAngles") final String R_AllAngles) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_tilt/GBM_model_python_1600930320161_670.zip"));

        RowData row = new RowData();
        row.put("T_AllowDiff", T_AllowDiff);
        row.put("R_HeightDiff", R_HeightDiff);
        row.put("R_ArrRectHeight1", R_ArrRectHeight1);
        row.put("R_ArrRectHeight2", R_ArrRectHeight2);
        row.put("R_ArrRectHeight3", R_ArrRectHeight3);
        row.put("R_ArrRectHeight4", R_ArrRectHeight4);
        row.put("R_HeightDiffMin", R_HeightDiffMin);
        row.put("R_HeightDiffMax", R_HeightDiffMax);
        row.put("T_Height", T_Height);
        row.put("T_HeightMin", T_HeightMin);
        row.put("T_HeightMax", T_HeightMax);
        row.put("R_AllAngles", R_AllAngles);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
