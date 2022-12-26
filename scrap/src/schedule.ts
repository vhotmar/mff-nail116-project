// @ts-ignore
import createClient from 'hafas-client'
// @ts-ignore
import hafasDBProfile from 'hafas-client/p/db/index.js'
import { CLIENT_NAME, ONLY_LOCAL_LINES } from './config.js'
import util from 'util'
import { z } from 'zod'
import { parseISO } from 'date-fns'
import { cachedFn } from './cache.js'

const client = createClient(hafasDBProfile, CLIENT_NAME)

export const StringDateZ = z.string().transform(date => parseISO(date))

export const LocationZ = z.object({
    type: z.literal('location'),
    id: z.string(),
    latitude: z.number(),
    longitude: z.number()
}).strict()

export const ProductsZ = z.object({
    nationalExpress: z.boolean(),
    national: z.boolean(),
    regionalExp: z.boolean(),
    regional: z.boolean(),
    suburban: z.boolean(),
    bus: z.boolean(),
    ferry: z.boolean(),
    subway: z.boolean(),
    tram: z.boolean(),
    taxi: z.boolean()
}).strict()

export const StationZ = z.object({
    type: z.literal('station'),
    id: z.string(),
    name: z.string(),
    location: LocationZ,
    products: ProductsZ
})

export const StopZ = z.object({
    type: z.string(),
    id: z.string(),
    name: z.string(),
    location: LocationZ,
    products: ProductsZ,
    station: StationZ.optional()
}).strict()

export const OperatorZ = z.object({
    type: z.literal('operator'),
    id: z.string(),
    name: z.string(),
}).strict()

export const ProductZ = z.union([
    z.literal('nationalExpress'),
    z.literal('national'),
    z.literal('regionalExp'),
    z.literal('regional'),
    z.literal('suburban'),
    z.literal('bus'),
    z.literal('ferry'),
    z.literal('subway'),
    z.literal('tram'),
    z.literal('taxi')
])

export const LineZ = z.object({
    type: z.literal('line'),
    id: z.string().nullable(),
    fahrtNr: z.string(),
    name: z.string().nullable(),
    public: z.boolean(),
    mode: z.literal('train'),
    product: ProductZ.nullable(),
    operator: OperatorZ,
    additionalName: z.string().optional()
}).strict()

export const RemarkZ = z.object({}).strict()

export const StopoverZ = z.object({
    stop: StopZ,
    arrival: StringDateZ.nullable(),
    arrivalDelay: z.null(),
    arrivalPlatform: z.null(),
    departure: StringDateZ.nullable(),
    departureDelay: z.null(),
    departurePlatform: z.string().nullable()
}).strict()

export const DepartureZ = z.object({
    tripId: z.string(),
    stop: StopZ,
    when: StringDateZ,
    direction: z.string(),
    line: LineZ,
    remarks: RemarkZ.array(),
    delay: z.null(),
    platform: z.string().nullable(),
    nextStopovers: StopoverZ.array(),
    loadFactor: z.string().optional()
}).strict()
export type DepartureZ = z.infer<typeof DepartureZ>

export const DeparturesZ = DepartureZ.array()

const clientDeparturesCached = cachedFn(
    'station-depatures',
    async (stationId: string, day: Date) => {
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
        })
    },
    (stationId: string, day: Date) => `departure-${stationId}-${Number(day)}`
)

export const fetchStationDaySchedule = async (stationId: string, day: Date) => {
    const departures = await clientDeparturesCached(stationId, day)

    const res = []

    for (const departure of departures) {
        try {
            res.push(DepartureZ.parse(departure))
        } catch (e) {
            console.log(util.inspect(departure, false, 10, true))
            throw e
        }
    }

    return res
}