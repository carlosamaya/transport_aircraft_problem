package com.github.carlosamaya

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


case class yearSchema(
                       fl_date: String,
                       op_carrier:	String,
                       op_carrier_fl_num: Int,
                       origin: String,
                       dest: String,
                       crs_dep_time: Int,
                       dep_time: Double,
                       dep_delay: Double,
                       taxi_out: Double,
                       wheels_off: Double,
                       wheels_on: Double,
                       taxi_in: Double,
                       crs_arr_time: Int,
                       arr_time: Double,
                       arr_delay: Double,
                       cancelled: Double,
                       cancellation_code: String,
                       diverted: Double,
                       crs_elapsed_time: Double,
                       actual_elapsed_time: Double,
                       air_time: Double,
                       distance: Double,
                       carrier_delay: Double,
                       weather_delay: Double,
                       nas_delay: Double,
                       security_delay: Double,
                       late_aircraft_delay: Double
                     )

case class airlineSchema(
                          iata_code: String,
                          airline: String
                        )

//1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?
//La respuesta debe incluir el nombre completo de la aerolínea, si no se envia el año debe calcular con
//toda la información disponible.

case class AirlineDelay(fl_date: String,
                        op_carrier: String,
                        origin: String,
                        dest: String,
                        dep_delay: Double,
                        arr_delay: Double)

case class AirlineStats(name: String,
                        totalFlights: Long,
                        largeDelayFlights: Long,
                        smallDelayFlights: Long,
                        onTimeFlights: Long)

object TransportProblem extends App with SparkSessionWrapper{
  val inputYearsFiles = "file:/Users/carlos.amaya/Documents/Develops/spark/Transport_Aircraft_Problem/transport_project/data/flight_data_years/"
  val inputDataFiles = "file:/Users/carlos.amaya/Documents/Develops/spark/Transport_Aircraft_Problem/transport_project/data/flight_delays/"

  val caseClassYearsSchema = Encoders.product[yearSchema].schema
  val caseClassAirlineSchema = Encoders.product[airlineSchema].schema

  val yearDetails = spark.read
    .option("header","true")
    .schema(caseClassYearsSchema)
    .csv( inputYearsFiles + "2018.csv", inputYearsFiles + "2017.csv", inputYearsFiles + "2016.csv")

  val airlineDetails = spark.read
    .option("header","true")
    .schema(caseClassAirlineSchema)
    .csv( inputDataFiles + "airlines.csv")

  //Se agrega el nombre de la Aerolinea
  val AirlineDelayDetails = yearDetails.join(airlineDetails)
                                       .where(yearDetails("op_carrier") === airlineDetails("iata_code"))
                                       .select(
                                         yearDetails("fl_date"),
                                         airlineDetails("airline"),
                                         yearDetails("origin"),
                                         yearDetails("dest"),
                                         yearDetails("dep_delay"),
                                         yearDetails("arr_delay")
                                       )

  val dsr = AirlineDelayDetails.select("airline","arr_delay")
                               .withColumn("onTimeFlights", col("arr_delay") < 5).reduce(_ + _)
                               .withColumn("smallDelayFlights", col("arr_delay") > 5 && col("arr_delay") < 45)
                               .withColumn("largelDelayFlights", col("arr_delay") > 45)
                               .drop("arr_delay")
                               .show()
/*
  val result = bestApps.join(reviews).where(bestApps("App") === reviews("AppName"))
    .filter($"Sentiment" === "Negative")
    .distinct()
    .sort($"Sentiment_Polarity".desc)
    .drop("AppName")

   * Un vuelo se clasifica de la siguiente manera:
   * ARR_DELAY < 5 min --- On time
   * 5 > ARR_DELAY < 45min -- small Delay
   * ARR_DELAY > 45min large delay
   *
   * Calcule por cada aerolinea el número total de vuelos durante el año
 (en caso de no recibir el parametro de todos los años)
   * y el número de ontime flights, smallDelay flighst y largeDelay flights
   *
   * Orderne el resultado por largeDelayFlights, smallDelayFlightsy, ontimeFlights
   *
   * @param ds
   * @param year
   */
  /*
  def delayedAirlines(ds: Dataset[AirlineDelay], year: Option[String]): Seq[AirlineStats] = {
    val dsr = ds.groupBy("airline")
                .agg(sort_array(collect_list("fl_date"), asc = false))
                .withColumn("onTimeFlights", col("arr_delay") < 5)
                .withColumn("smallDelayFlights", col("arr_delay") > 5 && col("arr_delay") < 45)
                .withColumn("largelDelayFlights", col("arr_delay") > 45)
                .show()


  }*/
  /*
    AirlineDelayDetails.show()
  yearDetails.show()
  airlineDetails.show()
  */

} 