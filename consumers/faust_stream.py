"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.station.arrivals.", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# TODO: Define a Faust Table
table = app.Table(
    "transformedStations",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transformedStations(stations):
    async for station in stations:
        logger.info(f"Evaluating station {station.station_id}")
        transformedStation = table[station.station_id]
        line = "RED" if station.red else "BLUE" if station.blue else "GREEN"
        transformedStation = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )

        table[station.station_id] = transformedStation
        logger.info(f"Station {station.station_id} transformed data sent to table")


if __name__ == "__main__":
    app.main()
