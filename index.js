#!/usr/bin/env/node
const url = require("url");
const util = require("util");

const Bottleneck = require("bottleneck");
const lockingCache = require("locking-cache");
const tilelive = require("tilelive-streaming")(
  require("tilelive-cache")(require("@mapbox/tilelive"))
);

// load and initialize available tilelive modules
require("tilelive-modules/loader")(tilelive, {
  "tilelive-http": {
    retry: true
  }
});

// handle unhandled rejections
process.on("unhandledRejection", err => {
  console.error(err);
  process.exit(1);
});

const load = util.promisify(tilelive.load);
const locker = lockingCache();

const fallback = (primary, cache) => {
  const getTile = locker((z, x, y, lock) => {
    return lock(primary.sourceURI + ":" + [z, x, y].join("/"), unlock => {
      return cache.getTile(z, x, y, (err, data, headers) => {
        // TODO apply cache invalidation
        if (err) {
          return primary.getTile(z, x, y, unlock);
        }

        return unlock(err, data, headers);
      });
    });
  });

  return {
    ...primary,
    getTile
  };
};

const rateLimit = source => {
  if (source.rateLimited) {
    return source;
  }

  const limiter = new Bottleneck(16, 100);

  const getTile = (z, x, y, callback) => {
    console.log([z, x, y].join("/"))
    return limiter.submit(source.getTile, z, x, y, callback);
  };

  return {
    ...source,
    getTile,
    rateLimited: true
  }
}

const copy = async (from, to, options) => {
  const [actualSource, sink] = await Promise.all([load(from), load(to)]);

  const cache = await load({
    ...url.parse("leveldb+file://./cache.db"),
    query: {
      id: from
    }
  });
  // TODO register something that periodically invalidates data (using same cache invalidation rules from ^^) if one hasn't already been registered
  const cacheWriter = cache.createWriteStream();

  // limit concurrency at the source level
  const rateLimitedSource = rateLimit(actualSource);
  const source = fallback(rateLimitedSource, cache);

  const rs = source.createReadStream(options);
  const ws = sink.createWriteStream();

  return new Promise((resolve, reject) => {
    rs.pipe(cacheWriter)
      .on("error", console.warn);

    rs
      .pipe(ws)
      .on("finish", resolve)
      .on("error", reject);
  });
};

const main = async () => {
  try {
    await Promise.all([
      copy(
        "http://tile.openstreetmap.org/{z}/{x}/{y}.png",
        // "mbtiles://./osm.mbtiles",
        "file://./osm/",
        {
          minzoom: 7,
          maxzoom: 7,
          bounds: [-121.4024, 43.9992, -121.2483, 44.125]
        }
      ),
      copy(
        "http://tile.openstreetmap.org/{z}/{x}/{y}.png",
        "mbtiles://./osm.mbtiles",
        {
          minzoom: 6,
          maxzoom: 7,
          bounds: [-121.4024, 43.9992, -121.2483, 44.125]
        }
      ),
    ]);

    // TODO copy MBTiles from a temporary location when complete
    // TODO notify a web hook when complete
    console.log("done.");
  } catch (err) {
    console.warn(err);
  }
};

main();
