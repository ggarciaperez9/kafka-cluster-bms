package com.allcircuits.kafka.udf;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.io.IOException;

@UdfDescription(
        name = "coyote_bodyblob_classify",
        description = "classify Coyote windows using algo_bodyblob AOI's table",
        version = "0.1.0",
        author = "Gabriel Perez"
)
public class CoyoteBodyblobClassifier {

    @Udf(description = "Apply classification model to algo_bodyblob table of Coyote")
    public int coyote_bodyblob_classify(
            @UdfParameter(value = "T_ShiftX", description = "the value of T_ShiftX") final double T_ShiftX,
            @UdfParameter(value = "T_ShiftY", description = "the value of T_ShiftY") final double T_ShiftY,
            @UdfParameter(value = "T_WidthRateMin", description = "the value of T_WidthRateMin") final double T_WidthRateMin,
            @UdfParameter(value = "T_WidthRateMax", description = "the value of T_WidthRateMax") final double T_WidthRateMax,
            @UdfParameter(value = "T_LengthRateMin", description = "the value of T_LengthRateMin") final double T_LengthRateMin,
            @UdfParameter(value = "T_LengthRateMax", description = "the value of T_LengthRateMax") final double T_LengthRateMax,
            @UdfParameter(value = "R_Width", description = "the value of R_Width") final double R_Width,
            @UdfParameter(value = "R_Length", description = "the value of R_Length") final double R_Length,
            @UdfParameter(value = "R_ShiftX", description = "the value of R_ShiftX") final double R_ShiftX,
            @UdfParameter(value = "R_ShiftY", description = "the value of R_ShiftY") final double R_ShiftY,
            @UdfParameter(value = "R_Angle", description = "the value of R_Angle") final double R_Angle,
            @UdfParameter(value = "T_Height", description = "the value of T_Height") final double T_Height,
            @UdfParameter(value = "T_HeightMax", description = "the value of T_HeightMax") final double T_HeightMax,
            @UdfParameter(value = "T_HeightMin", description = "the value of T_HeightMin") final double T_HeightMin,
            @UdfParameter(value = "R_HeightMean", description = "the value of R_HeightMean") final double R_HeightMean,
            @UdfParameter(value = "R_Area", description = "the value of R_Area") final double R_Area,
            @UdfParameter(value = "T_AreaRateMin", description = "the value of T_AreaRateMin") final double T_AreaRateMin,
            @UdfParameter(value = "T_AreaRateMax", description = "the value of T_AreaRateMax") final double T_AreaRateMax) throws PredictException, IOException {

        EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load("/home/gabriel/Models/PEM_POST_COY229564254/algo_bodyblob/GBM_model_python_1600342804708_84.zip"));

        RowData row = new RowData();
        row.put("T_ShiftX", T_ShiftX);
        row.put("T_ShiftY", T_ShiftY);
        row.put("T_WidthRateMin", T_WidthRateMin);
        row.put("T_WidthRateMax", T_WidthRateMax);
        row.put("T_LengthRateMin", T_LengthRateMin);
        row.put("T_LengthRateMax", T_LengthRateMax);
        row.put("R_Width", R_Width);
        row.put("R_Length", R_Length);
        row.put("R_ShiftX", R_ShiftX);
        row.put("R_ShiftY", R_ShiftY);
        row.put("R_Angle", R_Angle);
        row.put("T_Height", T_Height);
        row.put("T_HeightMax", T_HeightMax);
        row.put("T_HeightMin", T_HeightMin);
        row.put("R_HeightMean", R_HeightMean);
        row.put("R_Area", R_Area);
        row.put("T_AreaRateMin", T_AreaRateMin);
        row.put("T_AreaRateMax", T_AreaRateMax);

        BinomialModelPrediction p = model.predictBinomial(row);
        return p.label.equals("true") ? 1 : 0;
    }
}
