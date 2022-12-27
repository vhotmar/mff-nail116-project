// @ts-ignore
import createClient from "hafas-client";
// @ts-ignore
import hafasDBProfile from "hafas-client/p/db/index.js";
import { CLIENT_NAME, ONLY_LOCAL_LINES } from "../config.js";
import util from "util";
import { DepartureZ } from "./types.js";
import { z } from "zod";

const client = createClient(hafasDBProfile, CLIENT_NAME);

export const fetchStationDeparturesForDay = async (
  stationId: string,
  day: Date
) => {
  return await client.departures(stationId, {
    when: day,
    duration: 24 * 60,
    products: {
      nationalExpress: !ONLY_LOCAL_LINES,
      national: !ONLY_LOCAL_LINES,
      regionalExp: true,
      regional: true,
      suburban: true,
      bus: false,
      ferry: false,
      subway: false,
      tram: false,
      taxi: false,
    },
    stopovers: true,
    remarks: false,
  });
};

const HafasErrorZ = z.object({ isHafasError: z.literal(true) });

const ignoreHafasErrors = async <T>(
  promise: Promise<T>
): Promise<T | undefined> => {
  try {
    const data = await promise;

    return data;
  } catch (err) {
    if (HafasErrorZ.safeParse(err).success) {
      console.error(`Hafas error occured`, err);

      return undefined;
    }

    throw err;
  }
};

export const streamStationDaySchedule = async function* (
  stationId: string,
  day: Date
): AsyncIterable<DepartureZ> {
  console.log(`Fetching for station ${stationId} and day ${day}`);
  const departures = await ignoreHafasErrors(
    fetchStationDeparturesForDay(stationId, day)
  );
  console.log(`Fetched for station ${stationId} and day ${day}`);

  if (departures == null) return;

  for (const departure of departures) {
    try {
      yield DepartureZ.parse(departure);
    } catch (error) {
      console.error(util.inspect(departure, false, 10, true));
      console.error(`Failed to parse departure`, error);
    }
  }
};
