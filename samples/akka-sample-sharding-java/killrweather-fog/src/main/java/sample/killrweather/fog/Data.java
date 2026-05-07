package sample.killrweather.fog;

/**
 * Datapoint for serializing to JSON with jackson and posting to Killrweather HTTP API
 */
public class Data {
  public final long eventTime;
  public final String dataType;
  public final double value;

  public Data(long eventTime, String dataType, double value) {
    this.eventTime = eventTime;
    this.dataType = dataType;
    this.value = value;
  }
}
