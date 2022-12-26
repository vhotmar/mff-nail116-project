import createClient from 'hafas-client'
import hafasDBProfile from 'hafas-client/p/db/index.js'


const FETCH_STATIONS = false
const CLIENT_NAME = 'scrapper.NAIL116.mff'
const START_DATE = new Date()
const HOW_MANY_DAYS = 7
const ONLY_LOCAL_LINES = false

const getStations = async () => {
    if (FETCH_STATIONS) {
        const trainlineStations = await import('trainline-stations')

        return await trainlineStations.collect(trainlineStations.default())
    }

    return (await import('trainline-stations/src/static.js')).default
}

console.log('getting stations')
// const stations = await getStations()

console.log('got stations')

const client = createClient(hafasDBProfile, CLIENT_NAME)

const departures = await client.departures('8700023', {
    when: START_DATE, duration: 24 * 60, products: {
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

console.log(departures[0].nextStopovers)
