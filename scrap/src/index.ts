import fs from "fs/promises";
import { filter, map, pipe, toAsync } from "iter-ops";
import { z } from "zod";
import {
  GRAPH_JSON,
  HOW_MANY_DAYS_TO_FETCH,
  PROGRESS_JSON,
  START_DATE,
} from "./config.js";
import { streamStationDaySchedule } from "./hafas-client/index.js";
import { streamStations } from "./stations.js";
import { iterableToArray, pool } from "./utils/index.js";
import { CountryInfo, findCountryByCoordinate } from "country-locator";

const coordinatesMemo: Record<string, CountryInfo | undefined> = {};

const findCountryByCoordinateMemoized = (
  point: number[]
): CountryInfo | undefined => {
  const key = point.join("_");

  if (!(key in coordinatesMemo)) {
    console.log("Unfortunate lookup findCountryByCoordinateMemoized");
    coordinatesMemo[key] = findCountryByCoordinate(point);
  }

  return coordinatesMemo[key];
};

const stationsToCountry: Record<string, string | undefined> = {};

const getStationCountry = (stationId: string, point?: number[]) => {
  if (!(stationId in stationsToCountry) && point != null) {
    console.log("Unfortunate lookup getStationCountry");
    stationsToCountry[stationId] = findCountryByCoordinateMemoized(point)?.code;
  }

  return stationsToCountry[stationId];
};

export const StationZ = z.object({
  id: z.string(),
  name: z.string(),
  position: z
    .object({
      lat: z.number(),
      long: z.number(),
    })
    .optional(),
  country: z.string().optional(),
});
export type StationZ = z.infer<typeof StationZ>;

export const streamProcessedStations = (): AsyncIterable<StationZ> =>
  pipe(
    streamStations(),
    map((station): StationZ | undefined => {
      if (station.db_id == null) return undefined;

      return {
        name: station.name,
        position:
          station.longitude != null && station.latitude != null
            ? {
                lat: station.latitude,
                long: station.longitude,
              }
            : undefined,
        id: station.db_id,
        country: station.country,
      };
    }),
    filter((item): item is StationZ => item != null)
  );

export const ReachableZ = z.object({
  station: StationZ,
  line: z.string().optional(),
  direct: z.boolean(),
  duration: z.number(),
});
export type ReachableZ = z.infer<typeof ReachableZ>;

export const StationWithReachablesZ = z
  .object({
    station: StationZ,
    reachables: ReachableZ.array().optional(),
  })
  .strict();
export type StationWithReachablesZ = z.infer<typeof StationWithReachablesZ>;

export const streamReachableStationsFromStation = async function* (
  stationId: string,
  date: Date
): AsyncIterable<ReachableZ> {
  const departures = pipe(
    streamStationDaySchedule(stationId, date),
    filter(
      (departure) =>
        departure.line.mode === "train" &&
        (departure.line.name || "").slice(0, 3).toLowerCase() !== "bus"
    )
  );

  for await (const departure of departures) {
    let shouldSkip = true;
    let i = 0;

    for (const stopover of departure.nextStopovers) {
      if (shouldSkip) {
        if (stopover.stop.id === stationId) {
          shouldSkip = false;
        }

        continue;
      }

      if (stopover.arrival == null) {
        console.error(
          `Should not happen - arrival null (stationId: ${stationId})`
        );
        continue;
      }

      const duration =
        (stopover.arrival.valueOf() - departure.when.valueOf()) / (1000 * 60);

      if (duration <= 0 || duration / 60 > 210) {
        console.error(
          `Should not happen - duration weird: ${duration} (stationId: ${stationId})`
        );
        continue;
      }

      yield {
        station: {
          id: stopover.stop.id,
          name: stopover.stop.name,
          position: {
            lat: stopover.stop.location.latitude,
            long: stopover.stop.location.longitude,
          },
          country: getStationCountry(stopover.stop.id, [
            stopover.stop.location.longitude,
            stopover.stop.location.latitude,
          ]),
        },
        line: departure.line.id ?? undefined,
        direct: i === 0,
        duration,
      };

      i += 1;
    }
  }
};

export const streamReachableStations = async function* (
  stations: AsyncIterable<StationZ>,
  day: number
): AsyncIterable<StationWithReachablesZ> {
  let stationsFetched = 0;

  yield* pool(
    stations,
    async (station): Promise<StationWithReachablesZ> => {
      try {
        stationsFetched += 1;

        if (stationsFetched % 100 == 0) {
          console.log(`Stations fetched ${stationsFetched} (day ${day})`);
        }

        return {
          station,
          reachables: await iterableToArray(
            streamReachableStationsFromStation(
              station.id,
              new Date(START_DATE.valueOf() + 24 * 60 * 60 * 1000 * day)
            )
          ),
        };
      } catch (error) {
        console.error(
          `Error during processing: ${station.id} (day ${day})`,
          error
        );

        return { station, reachables: undefined };
      }
    },
    8
  );
};

type Edge = { duration: number; direct: boolean };
type Graph = {
  adjacency: { [key: string]: { [key: string]: Edge } };
  nodes: { [key: string]: StationZ };
};

const getEdge = (graph: Graph, from: string, to: string) => {
  if (graph.nodes[from] == null)
    throw new Error(`from: ${from} missing in nodes`);
  if (graph.nodes[to] == null) throw new Error(`to: ${to} missing in nodes`);

  return (graph.adjacency[from] ??= {})[to];
};

const addEdge = (graph: Graph, from: string, to: string, edge: Edge) => {
  if (graph.nodes[from] == null)
    throw new Error(`from: ${from} missing in nodes`);
  if (graph.nodes[to] == null) throw new Error(`to: ${to} missing in nodes`);

  (graph.adjacency[from] ??= {})[to] ??= edge;
};

const addEdgeIfShorter = (
  graph: Graph,
  from: string,
  to: string,
  edge: Edge
) => {
  const e = getEdge(graph, from, to);

  if (e == null) {
    addEdge(graph, from, to, edge);
    return;
  }

  // we want to know whether there is a direct connection (we do not care if it is the shortest)
  e.direct = e.direct || edge.direct;

  if (e.duration > edge.duration) {
    e.duration = edge.duration;
  }
};

const addVertex = (graph: Graph, vertex: StationZ) => {
  graph.nodes[vertex.id] = {
    id: vertex.id,
    name: graph.nodes[vertex.id]?.name ?? vertex.name,
    position: graph.nodes[vertex.id]?.position ?? vertex.position,
    country: graph.nodes[vertex.id]?.country ?? vertex.country,
  };
};

export const updateDistanceStationGraph = (
  graph: Graph,
  data: StationWithReachablesZ
) => {
  addVertex(graph, data.station);

  for (const reachable of data.reachables ?? []) {
    addVertex(graph, reachable.station);

    addEdgeIfShorter(graph, data.station.id, reachable.station.id, {
      duration: reachable.duration,
      direct: reachable.direct,
    });
  }
};

export const tryToLoadGraph = async (): Promise<Graph> => {
  try {
    return JSON.parse(String(await fs.readFile(GRAPH_JSON)));
  } catch (error) {
    console.error(`Could not load graph from json`, error);
    return { adjacency: {}, nodes: {} };
  }
};

type Progress = {
  days: { [day: number]: { [station: string]: boolean } };
};

export const tryToLoadProgress = async (): Promise<Progress> => {
  try {
    return JSON.parse(String(await fs.readFile(PROGRESS_JSON)));
  } catch (error) {
    console.error(`Could not load progress from json`, error);
    return { days: {} };
  }
};

export const saveGraphAndProgress = async (
  graph: Graph,
  progress: Progress
) => {
  console.log("Saving progress information");

  const graphString = JSON.stringify(graph);
  const progressString = JSON.stringify(progress);

  await Promise.all([
    fs.writeFile(`${GRAPH_JSON}-${Date.now()}`, graphString),
    fs.writeFile(`${PROGRESS_JSON}-${Date.now()}`, progressString),
  ]);

  await Promise.all([
    fs.writeFile(GRAPH_JSON, graphString),
    fs.writeFile(PROGRESS_JSON, progressString),
  ]);

  console.log("Saved progress information");
};

export const main = async () => {
  console.log("Loading downloaded data");
  const graph: Graph = await tryToLoadGraph();
  const progress: Progress = await tryToLoadProgress();
  console.log("Loading stations");
  const stations = await iterableToArray(streamProcessedStations());
  console.log(`${stations.length} loaded`);

  const stationIds = new Set(stations.map(station => station.id))

  Object.values(graph.nodes).forEach(node => {
    if (!stationIds.has(node.id)) {
      stations.push(node)
    }
  })

  console.log(`${stations.length} after graph add`);

  stations.forEach((station) => {
    if (station.country != null) {
      stationsToCountry[station.id] = station.country;
    }
  });
  console.log("Loaded stations");
  let i = 0;

  for (let day = 0; day < HOW_MANY_DAYS_TO_FETCH; day++) {
    const unprocessedStations = stations.filter(
      (station) => (progress.days[day] ??= {})[station.id] !== true
    );

    console.log(
      `Processing day ${day}/${HOW_MANY_DAYS_TO_FETCH}, remaining stations ${unprocessedStations.length}`
    );

    for await (const item of streamReachableStations(
      toAsync(unprocessedStations),
      day
    )) {
      if (item.reachables != null) {
        updateDistanceStationGraph(graph, item);
        (progress.days[day] ??= {})[item.station.id] = true;
      }

      i += 1;

      if (i % 500 == 0) {
        await saveGraphAndProgress(graph, progress);
      }
    }

    console.log("LOOP ENDED");
  }

  console.log("DONE");

  await saveGraphAndProgress(graph, progress);
  //}
};

main()
  .then(() => console.log("Everything completed"))
  .catch((error) => console.error("ERROR", error));
