import fs from 'fs'
import { concat, page, pipe, toAsync } from 'iter-ops';
import PQueue from 'p-queue'
import { z } from 'zod';
import { FETCH_STATIONS, HOW_MANY_DAYS_TO_FETCH, START_DATE } from './config.js'
import { DepartureZ, fetchStationDaySchedule } from './schedule.js';
import { streamStations } from './stations.js'

export const StationZ = z.object({
    id: z.string(),
    name: z.string(),
    position: z.object({
        lat: z.number(),
        long: z.number()
    }).optional(),
})
export type StationZ = z.infer<typeof StationZ>

export const fetchStations = async (): Promise<StationZ[]> => {
    const stations: StationZ[] = [];

    for await (const station of streamStations()) {
        if (station.db_id == null) continue;

        stations.push({
            name: station.name,
            position: station.longitude != null && station.latitude != null ? {
                lat: station.latitude,
                long: station.longitude
            } : undefined,
            id: station.db_id,
        })
    }

    return stations
}

export const getStations = async (): Promise<StationZ[]> => {
    const fileName = './stations.json'

    if (fs.existsSync(fileName)) {
        return StationZ.array().parse(JSON.parse(String(fs.readFileSync(fileName))));
    }

    const stations = await fetchStations()

    fs.writeFileSync(fileName, JSON.stringify(stations))

    return stations
}

export const ReachableZ = z.object({
    station: StationZ,
    line: z.string().optional(),
    direct: z.boolean(),
    duration: z.number()
})
export type ReachableZ = z.infer<typeof ReachableZ>

export const StationWithReachablesZ = z.object({
    station: StationZ,
    reachables: ReachableZ.array().optional()
}).strict()
export type StationWithReachablesZ = z.infer<typeof StationWithReachablesZ>

export const fetchReachableStationsFromStation = async (stationId: string, date: Date): Promise<ReachableZ[]> => {
    const departures = await fetchStationDaySchedule(stationId, date)
    const trainDepartures = departures.filter(departure => departure.line.mode === 'train' && (departure.line.name || '').slice(0, 3).toLowerCase() !== 'bus')

    const reachable: ReachableZ[] = []

    for (const departure of trainDepartures) {
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
                console.error(`Should not happen - arrival null (stationId: ${stationId})`);
                continue;
            }

            const duration = (stopover.arrival.valueOf() - departure.when.valueOf()) / (1000 * 60)

            if (duration <= 0 || (duration / 60) > 210) {
                console.error(`Should not happen - duration weird: ${duration} (stationId: ${stationId})`);
                continue;

            }

            reachable.push({
                station: {
                    id: stopover.stop.id,
                    name: stopover.stop.name,
                    position: { lat: stopover.stop.location.latitude, long: stopover.stop.location.longitude }
                },
                line: departure.line.id ?? undefined,
                direct: i === 0,
                duration
            })

            i += 1;
        }
    }

    return reachable;
}

export const splitToParts = <T>(array: T[], parts: number): T[][] => {
    const size = Math.ceil(array.length / parts);
    return Array.from({ length: parts }, (v, i) =>
        array.slice(i * size, i * size + size)
    );
}

export const simpleIteratorParallel = async function *<T, R>(jobs: T[], jobFn: (data: T) => Promise<R>, parallelism: number): AsyncIterable<R> {
    for (const jobsPage of pipe(jobs, page(parallelism))) {
        yield * (await Promise.all(jobsPage.map((job) => jobFn(job))))
    }
}

export const fetchReachableStations = async function* (stations: StationZ[], day: number): AsyncIterable<StationWithReachablesZ> {
    let stationsFetched = 0;

    yield* simpleIteratorParallel(stations, async (station): Promise<StationWithReachablesZ> => {
        try {
            stationsFetched += 1

            if (stationsFetched % 100 == 0) {
                console.log(`Stations ${stationsFetched}/${stations.length} (day ${day})`)
            }

            return { station, reachables: await fetchReachableStationsFromStation(station.id, new Date(START_DATE.valueOf() + 24 * 60 * 60 * 1000 * day)) }
        } catch (e) {
            console.error(`Error during processing: ${station.id}`)
            console.error(e)

            return { station, reachables: undefined }
        }
    }, 8)
}

type Graph = {
    adjacency: { [key: string]: { [key: string]: number } },
    nodes: { [key: string]: StationZ },
}


const getEdge = (graph: Graph, from: string, to: string) => {
    if (graph.nodes[from] == null) throw new Error(`from: ${from} missing in nodes`)
    if (graph.nodes[to] == null) throw new Error(`to: ${to} missing in nodes`)

    graph.adjacency[from] ||= {}

    return graph.adjacency[from][to];
}

const addEdge = (graph: Graph, from: string, to: string, edge: number) => {
    if (graph.nodes[from] == null) throw new Error(`from: ${from} missing in nodes`)
    if (graph.nodes[to] == null) throw new Error(`to: ${to} missing in nodes`)

    graph.adjacency[from] ||= {}
    graph.adjacency[from][to] ||= edge
}

const addEdgeIfShorter = (graph: Graph, from: string, to: string, edge: number) => {
    const e = getEdge(graph, from, to)

    if (e == null) {
        addEdge(graph, from, to, edge);
        return;
    }

    if (e > edge) {
        addEdge(graph, from, to, edge);
    }
}

const addVertex = (graph: Graph, vertex: StationZ) => {
    graph.nodes[vertex.id] = vertex
}

export const updateDistanceStationGraph = (graph: Graph, data: StationWithReachablesZ, onlyDirect = false) => {
    addVertex(graph, data.station)

    for (const reachable of (data.reachables ?? [])) {
        addVertex(graph, reachable.station)

        if (onlyDirect) {
            if (reachable.direct)
                addEdgeIfShorter(graph, data.station.id, reachable.station.id, reachable.duration)
        } else {
            addEdgeIfShorter(graph, data.station.id, reachable.station.id, reachable.duration)
        }
    }
}

export const main = async () => {
    const stations = await getStations();

    console.log(`Stations count: ${stations.length}`)

    //for (let i = 0; i < HOW_MANY_DAYS_TO_FETCH; i++) {
    const graph: Graph = { adjacency: {}, nodes: {} }

    for await (const item of fetchReachableStations(stations, 0)) {
        updateDistanceStationGraph(graph, item)
    }

    fs.writeFileSync('./graph_all.json', JSON.stringify(graph))
    //}
}

main()