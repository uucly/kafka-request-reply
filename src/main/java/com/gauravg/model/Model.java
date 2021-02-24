
package com.gauravg.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "firstNumber",
        "secondNumber"
})
public class Model {

    @JsonProperty("firstNumber")
    private int firstNumber;
    @JsonProperty("secondNumber")
    private int secondNumber;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("firstNumber")
    public int getFirstNumber() {
        return firstNumber;
    }

    @JsonProperty("firstNumber")
    public void setFirstNumber(int firstNumber) {
        this.firstNumber = firstNumber;
    }

    @JsonProperty("secondNumber")
    public int getSecondNumber() {
        return secondNumber;
    }

    @JsonProperty("secondNumber")
    public void setSecondNumber(int secondNumber) {
        this.secondNumber = secondNumber;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        return "Model{" +
                "firstNumber=" + firstNumber +
                ", secondNumber=" + secondNumber +
                ", additionalProperties=" + additionalProperties +
                '}';
    }
}
