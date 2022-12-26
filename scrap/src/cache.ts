import { FileSystemCache as Cache } from "file-system-cache"

export const cachedFn = <TArgs extends unknown[], TReturn>(
    namespace: string,
    fn: (...args: TArgs) => Promise<TReturn>,
    keyFn: (...args: TArgs) => string
): (...args: TArgs) => Promise<TReturn> => {

    const cache = new Cache({
        basePath: './.cache',
        ns: namespace
    });

    return async (...args: TArgs): Promise<TReturn> => {
        const key = keyFn(...args);
        const possibleRes = await cache.get(key);

        if (possibleRes != null) return possibleRes;

        const res = await fn(...args);

        await cache.set(key, res);

        return res;
    }
}