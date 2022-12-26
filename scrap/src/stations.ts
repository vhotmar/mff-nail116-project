import { parse } from "csv-parse"
import got from "got"
import { map, pipe } from "iter-ops"
import {z} from 'zod'

export const StationZ = z.object({
    id: z.string(),
    db_id: z.string().optional(),
    name: z.string(),
    latitude: z.string().transform(x => Number(x)).optional(),
    longitude: z.string().transform(x => Number(x)).optional(),
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
    'info:cs': z.string().optional(),
})
export type StationZ = z.infer<typeof StationZ>

export const streamStationsRaw = () => got
    .stream('https://raw.githubusercontent.com/trainline-eu/stations/master/stations.csv')
    .pipe(parse({
        columns: true,
        skip_empty_lines: true,
        delimiter: ';',
        cast: (value: any) => {
            if (value === '') return undefined
            if (value === 't') return true
            if (value === 'f') return false
            return value
        },
    }))

export const streamStations = () => {
    return pipe(streamStationsRaw(), map(item => StationZ.parse(item)))
}