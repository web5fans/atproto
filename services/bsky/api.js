/* eslint-env node */
/* eslint-disable import/order */

'use strict'

const dd = require('dd-trace')

dd.tracer
  .init()
  .use('http2', {
    client: true, // calls into dataplane
    server: false,
  })
  .use('express', {
    hooks: {
      request: (span, req) => {
        maintainXrpcResource(span, req)
      },
    },
  })

// modify tracer in order to track calls to dataplane as a service with proper resource names
const DATAPLANE_PREFIX = '/bsky.Service/'
const origStartSpan = dd.tracer._tracer.startSpan
dd.tracer._tracer.startSpan = function (name, options) {
  if (
    name !== 'http.request' ||
    options?.tags?.component !== 'http2' ||
    !options?.tags?.['http.url']
  ) {
    return origStartSpan.call(this, name, options)
  }
  const uri = new URL(options.tags['http.url'])
  if (!uri.pathname.startsWith(DATAPLANE_PREFIX)) {
    return origStartSpan.call(this, name, options)
  }
  options.tags['service.name'] = 'dataplane-bsky'
  options.tags['resource.name'] = uri.pathname.slice(DATAPLANE_PREFIX.length)
  return origStartSpan.call(this, name, options)
}

// Tracer code above must come before anything else
const assert = require('node:assert')
const cluster = require('node:cluster')
const path = require('node:path')

const bsky = require('@atproto/bsky')
const { Secp256k1Keypair } = require('@atproto/crypto')

const main = async () => {
  const env = getEnv()
  const config = bsky.ServerConfig.readEnv()
  assert(env.serviceSigningKey, 'must set BSKY_SERVICE_SIGNING_KEY')
  const signingKey = await Secp256k1Keypair.import(env.serviceSigningKey)
  // starts: involve logics in packages/dev-env/src/bsky.ts >>>>>>>>>>>>>
  // Separate migration db in case migration changes some connection state that we need in the tests, e.g. "alter database ... set ..."
  const migrationDb = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
  })
  if (env.migration) {
    await migrationDb.migrateToOrThrow(env.migration)
  } else {
    await migrationDb.migrateToLatestOrThrow()
  }
  await migrationDb.close()

  const db = new bsky.Database({
    url: env.dbPostgresUrl,
    schema: env.dbPostgresSchema,
    poolSize: 20,
  })

  const dataplane = await bsky.DataPlaneServer.create(
    db,
    env.dataplanePort,
    config.didPlcUrl,
  )

  const bsync = await bsky.MockBsync.create(db, env.bsyncPort)

  // ends: involve logics in packages/dev-env/src/bsky.ts   <<<<<<<<<<<<<

  const server = bsky.BskyAppView.create({ config, signingKey })
  // starts: involve logics in packages/dev-env/src/bsky.ts >>>>>>>>>>>>>
  const sub = new bsky.RepoSubscription({
    service: env.repoProvider,
    db,
    idResolver: dataplane.idResolver,
    background: new bsky.BackgroundQueue(db),
  })
  // ends: involve logics in packages/dev-env/src/bsky.ts   <<<<<<<<<<<<<
  await server.start()
  sub.start() // involve logics in packages/dev-env/src/bsky.ts
  // Graceful shutdown (see also https://aws.amazon.com/blogs/containers/graceful-shutdowns-with-ecs/)
  const shutdown = async () => {
    await server.destroy()
    await bsync.destroy()
    await dataplane.destroy()
    await sub.destroy()
    await db.close()
  }
  process.on('SIGTERM', shutdown)
  process.on('disconnect', shutdown) // when clustering
}

const getEnv = () => ({
  serviceSigningKey: process.env.BSKY_SERVICE_SIGNING_KEY || undefined,
  dbPostgresUrl: process.env.BSKY_DB_POSTGRES_URL || undefined,
  dbPostgresSchema: process.env.BSKY_DB_POSTGRES_SCHEMA || undefined,
  dataplanePort: maybeParseInt(process.env.BSKY_DATAPLANE_PORT) || undefined,
  bsyncPort: maybeParseInt(process.env.BSKY_BSYNC_PORT) || undefined,
  migration: process.env.ENABLE_MIGRATIONS === 'true' || undefined,
  repoProvider: process.env.BSKY_REPO_PROVIDER || undefined,
})

const maybeParseInt = (str) => {
  if (!str) return
  const int = parseInt(str, 10)
  if (isNaN(int)) return
  return int
}

const maintainXrpcResource = (span, req) => {
  // Show actual xrpc method as resource rather than the route pattern
  if (span && req.originalUrl?.startsWith('/xrpc/')) {
    span.setTag(
      'resource.name',
      [
        req.method,
        path.posix.join(req.baseUrl || '', req.path || '', '/').slice(0, -1), // Ensures no trailing slash
      ]
        .filter(Boolean)
        .join(' '),
    )
  }
}

const workerCount = maybeParseInt(process.env.CLUSTER_WORKER_COUNT)

if (workerCount) {
  if (cluster.isPrimary) {
    console.log(`primary ${process.pid} is running`)
    const workers = new Set()
    for (let i = 0; i < workerCount; ++i) {
      workers.add(cluster.fork())
    }
    let teardown = false
    cluster.on('exit', (worker) => {
      workers.delete(worker)
      if (!teardown) {
        workers.add(cluster.fork()) // restart on crash
      }
    })
    process.on('SIGTERM', () => {
      teardown = true
      console.log('disconnecting workers')
      workers.forEach((w) => w.disconnect())
    })
  } else {
    console.log(`worker ${process.pid} is running`)
    main()
  }
} else {
  main() // non-clustering
}
