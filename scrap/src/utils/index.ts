export { cachedFn } from "./cache.js";
export { pool } from "./parallel.js";

export async function iterableToArray<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of gen) {
    out.push(x);
  }
  return out;
}
