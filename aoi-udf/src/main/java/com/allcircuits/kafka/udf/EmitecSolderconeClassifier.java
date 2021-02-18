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
        name = "emitec_soldercone_classify",
        description = "classify Emitec windows using algo_soldercone AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class EmitecSolderconeClassifier {

    @Udf(description = "Apply classification model to algo_soldercone table of AOI")
    public int emitec_soldercone_classify(
            @UdfParameter(value = "T_Height", description = "the value of T_Height") final double T_Height,
            @UdfParameter(value = "T_Height1", description = "the value of T_Height1") final double T_Height1,
            @UdfParameter(value = "T_Height2", description = "the value of T_Height2") final double T_Height2,
            @UdfParameter(value = "T_Height3", description = "the value of T_Height3") final double T_Height3,
            @UdfParameter(value = "T_Percent1", description = "the value of T_Percent1") final double T_Percent1,
            @UdfParameter(value = "T_Percent2", description = "the value of T_Percent2") final double T_Percent2,
            @UdfParameter(value = "T_Percent3", description = "the value of T_Percent3") final double T_Percent3,
            @UdfParameter(value = "R_Area1", description = "the value of R_Area1") final double R_Area1,
            @UdfParameter(value = "R_Area2", description = "the value of R_Area2") final double R_Area2,
            @UdfParameter(value = "R_Area3", description = "the value of R_Area3") final double R_Area3) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_GU74712796CMS/algo_soldercone/GBM_model_python_1600930320161_798.zip"));

        RowData row = new RowData();
        row.put("T_Height", T_Height);
        row.put("T_Height1", T_Height1);
        row.put("T_Height2", T_Height2);
        row.put("T_Height3", T_Height3);
        row.put("T_Percent1", T_Percent1);
        row.put("T_Percent2", T_Percent2);
        row.put("T_Percent3", T_Percent3);
        row.put("R_Area1", R_Area1);
        row.put("R_Area2", R_Area2);
        row.put("R_Area3", R_Area3);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
