package org.kecak.topics.client.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class TopicVariables {
    final Map<String, String> variables = new HashMap<>();

    public Set<String> getVariables() {
        return variables.keySet();
    }

    public String getVariable(String name) {
        return variables.getOrDefault(name, "");
    }

    public void setVariable(String name, String value) {
        variables.put(name, value);
    }
}
