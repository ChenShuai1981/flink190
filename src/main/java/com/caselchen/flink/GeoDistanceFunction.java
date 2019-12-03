package com.caselchen.flink;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class GeoDistanceFunction extends ScalarFunction {
    public double eval(double lat1, double lon1, double lat2, double lon2) {
        return LocationUtils.getDistance(lat1, lon1, lat2, lon2);
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.DOUBLE;
    }
}
