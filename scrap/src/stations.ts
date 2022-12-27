import { parse } from "csv-parse";
import got from "got";
import { filter, map, pipe } from "iter-ops";
import { z } from "zod";

export const StationZ = z.object({
  id: z.string(),
  db_id: z.string().optional(),
  name: z.string(),
  latitude: z
    .string()
    .transform((x) => Number(x))
    .optional(),
  longitude: z
    .string()
    .transform((x) => Number(x))
    .optional(),
  parent_station_id: z.string().optional(),
  country: z.string(),
  time_zone: z.string(),
  is_city: z.boolean(),
  is_main_station: z.boolean(),
  is_airport: z.boolean(),
  is_suggestable: z.boolean(),
  country_hint: z.boolean(),
  main_station_hint: z.boolean(),
  db_is_enabled: z.boolean(),
  "info:cs": z.string().optional(),
});
export type StationZ = z.infer<typeof StationZ>;

/**
 * Get stream of stations (unvalidated objects)
 */
export const streamStationsRaw = () =>
  got
    .stream(
      "https://raw.githubusercontent.com/trainline-eu/stations/master/stations.csv"
    )
    .pipe(
      parse({
        columns: true,
        skip_empty_lines: true,
        delimiter: ";",
        cast: (value: any) => {
          if (value === "") return undefined;
          if (value === "t") return true;
          if (value === "f") return false;
          return value;
        },
      })
    );

/**
 * Validate the incoming stream
 */
export const streamStations = (): AsyncIterable<StationZ> => {
  return pipe(
    streamStationsRaw(),
    map((item): StationZ | undefined => {
      const res = StationZ.safeParse(item);

      if (res.success) return res.data;

      console.error(`Error during parsing of item`, { item });

      return undefined;
    }),
    filter((item): item is StationZ => item != null)
  );
};
