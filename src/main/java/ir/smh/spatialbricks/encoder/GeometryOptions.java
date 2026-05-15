package ir.smh.spatialbricks.encoder;

import java.io.Serializable;
import java.util.Set;

public class GeometryOptions implements Serializable {
    private final Set<String> features;

    public GeometryOptions(Set<String> features) {
        this.features = features;
    }

    public boolean has(String feature) {
        return features.contains(feature);
    }

    public static GeometryOptions of(String... features) {
        return new GeometryOptions(Set.of(features));
    }
}
