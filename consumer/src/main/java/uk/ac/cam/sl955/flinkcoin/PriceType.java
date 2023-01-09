package uk.ac.cam.sl955.flinkcoin;

public enum PriceType {
  ACTUAL("actual price"),
  ES_PRED("exponential smoothing prediction"),
  ARIMA_PRED("ARIMA prediction"),
  ES_ERR("exp smoothing error"),
  ARIMA_ERR("ARIMA error");

  private final String description;
  PriceType(String description) { this.description = description; }

}
