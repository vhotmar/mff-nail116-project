export const pool = async function* <T, R>(
  items: AsyncIterable<T>,
  process: (data: T) => Promise<R>,
  parallel: number
) {
  const iterator = items[Symbol.asyncIterator]();
  let idSequence = 1;
  const promisePool = new Map<number, Promise<{ value: R; id: number }>>();
  let nextPromise: Promise<{ done?: boolean }> | undefined = undefined;

  const getNextValue = async (): Promise<{ done?: boolean }> => {
    const result = await iterator.next();

    if (result.done !== true) {
      const id = ++idSequence;
      const promise = process(result.value).then((value) => ({ value, id }));
      promisePool.set(id, promise);
    }

    return { done: result.done };
  };

  const nextValuePromise = (): Promise<{ done?: boolean }> => {
    if (nextPromise == null)
      nextPromise = getNextValue().then((result) => {
        nextPromise = undefined;
        return result;
      });
    return nextPromise;
  };

  // done indicates whether we have already pulled every value from the items source
  // 1. If we are done, or if the pool is full we need to wait for a promise to complete
  // 2.
  let { done } = await nextValuePromise();
  while (done !== true || promisePool.size > 0) {
    if (done || promisePool.size === parallel) {
      const { value, id } = await Promise.race(promisePool.values());
      promisePool.delete(id);
      yield value;
    } else {
      const result = await Promise.race([
        ...promisePool.values(),
        nextValuePromise(),
      ]);

      if ("id" in result) {
        promisePool.delete(result.id);
        yield result.value;
      } else {
        done = result.done;
      }
      // if !id, then iterable.next() resolved, either a new Promise in promisePool or it is done
    }
  }
};
