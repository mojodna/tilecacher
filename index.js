#!/usr/bin/env/node
const url = require("url");
const util = require("util");

const lockingCache = require("locking-cache");
const tilelive = require("tilelive-streaming")(
  require("tilelive-cache")(require("@mapbox/tilelive"))
  // require("@mapbox/tilelive")
);

// load and initialize available tilelive modules
require("tilelive-modules/loader")(tilelive);

// handle unhandled rejections
process.on("unhandledRejection", err => {
  console.error(err);
  process.exit(1);
});

const load = util.promisify(tilelive.load);
const locker = lockingCache();

const fallback = (primary, cache, uri) => {
  const getTile = locker((z, x, y, lock) => {
    return lock(uri + ":" + [z, x, y].join("/"), unlock => {
      return cache.getTile(z, x, y, (err, data, headers) => {
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
}

const copy = async (from, to, options) => {
  const [actualSource, sink] = await Promise.all([load(from), load(to)]);

  const cache = await load({
    ...url.parse("leveldb+file://./cache.db"),
    query: {
      id: from
    }
  });
  const cacheWriter = cache.createWriteStream();

  const source = fallback(actualSource, cache, from);

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

    console.log("done.");
  } catch (err) {
    console.warn(err);
  }
};

main();
