package com.sportygroup.settlement.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SafeMap {

    private SafeMap() {}

    /**
     * Safely constructs an unmodifiable Map from alternating key-value pairs.
     * Null keys are safely converted to a descriptive fallback; null values are omitted rather than serialized as fake "null" strings.
     */
    public static Map<String, Object> of(Object... keyValues) {
        if (keyValues == null || keyValues.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = keyValues[i] != null ? keyValues[i].toString() : "key_" + (i / 2);
            Object value = (i + 1 < keyValues.length) ? keyValues[i + 1] : null;
            if (value != null) {
                map.put(key, value);
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
