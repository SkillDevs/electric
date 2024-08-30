import {
  ChangeMessage,
  ShapeStreamOptions,
  ShapeStream,
  Message,
} from "@electric-sql/client"
import {
  DatabaseAdapter,
} from "@electric-sql/drivers"
import { electricShapesTable } from "./schema"
import { Statement } from "@electric-sql/drivers/util"

export type MapColumnsMap = Record<string, string>
export type MapColumnsFn = (message: ChangeMessage<any>) => Record<string, any>
export type MapColumns = MapColumnsMap | MapColumnsFn
export interface SyncShapeToTableOptions extends ShapeStreamOptions {
  table: string
  schema?: string
  mapColumns?: MapColumns
  primaryKey: string[]
}

export type TableStreamApi = Awaited<ReturnType<typeof syncShapeToTable>>

const debug = true

export async function syncShapeToTable(
  streams: Array<{
    stream: ShapeStream
    aborter: AbortController
  }>,
  db: DatabaseAdapter,
  options: SyncShapeToTableOptions
) {
  const aborter = new AbortController()
  if (options.signal) {
    // we new to have our own aborter to be able to abort the stream
    // but still accept the signal from the user
    options.signal.addEventListener("abort", () => aborter.abort(), {
      once: true,
    })
  }
  const stream = new ShapeStream({
    ...options,
    signal: aborter.signal,
  })

  stream.subscribe(async (messages) => {
    if (debug) {
      console.log("sync messages received", messages)
    }

    const dlmStmts: Statement[] = []
    for (const message of messages) {
      const stmt = applyMessageToTable({
        rawMessage: message,
        table: options.table,
        schema: options.schema,
        mapColumns: options.mapColumns,
        primaryKey: options.primaryKey,
        debug,
      })
      if (stmt) {
        dlmStmts.push(stmt)
      }
    }

    await db.runInTransaction(
      ...dlmStmts,
      // cache the offset and shape id
      cacheShapeInfo(stream, options)
    )
  })
  streams.push({
    stream,
    aborter,
  })
  const unsubscribe = () => {
    stream.unsubscribeAll()
    aborter.abort()
  }
  return {
    unsubscribe,
    get isUpToDate() {
      return stream.isUpToDate
    },
    get shapeId() {
      return stream.shapeId
    },
    get lastOffset() {
      // @ts-ignore - this is incorrectly marked as private
      return stream.lastOffset
    },
    subscribeOnceToUpToDate: (cb: () => void, error: (err: Error) => void) => {
      return stream.subscribeOnceToUpToDate(cb, error)
    },
    unsubscribeAllUpToDateSubscribers: () => {
      stream.unsubscribeAllUpToDateSubscribers()
    },
  }
}

function doMapColumns(
  mapColumns: MapColumns,
  message: ChangeMessage<any>
): Record<string, any> {
  if (typeof mapColumns === "function") {
    return mapColumns(message)
  } else {
    const mappedColumns: Record<string, any> = {}
    for (const [key, value] of Object.entries(mapColumns)) {
      mappedColumns[key] = message.value[value]
    }
    return mappedColumns
  }
}

interface ApplyMessageToTableOptions {
  table: string
  schema?: string
  rawMessage: Message
  mapColumns?: MapColumns
  primaryKey: string[]
  debug: boolean
}

function applyMessageToTable({
  table,
  schema = "main",
  rawMessage,
  mapColumns,
  primaryKey,
  debug,
}: ApplyMessageToTableOptions): Statement | undefined {
  if (!(rawMessage as any).headers.operation) return undefined

  const message = rawMessage as ChangeMessage<any>
  const data = mapColumns ? doMapColumns(mapColumns, message) : message.value
  if (message.headers.operation === "insert") {
    if (debug) {
      console.log("inserting", data)
    }
    const columns = Object.keys(data)
    return {
      sql: `
        INSERT INTO "${schema}"."${table}"
        (${columns.map((s) => '"' + s + '"').join(", ")})
        VALUES
        (${columns.map((_v, i) => "$" + (i + 1)).join(", ")})
      `,
      args: columns.map((column) => data[column]),
    }
  } else if (message.headers.operation === "update") {
    if (debug) {
      console.log("updating", data)
    }
    const columns = Object.keys(data).filter(
      // we don't update the primary key, they are used to identify the row
      (column) => !primaryKey.includes(column)
    )
    if (columns.length > 0) {
      return {
        sql: `
        UPDATE "${schema}"."${table}"
        SET ${columns
          .map((column, i) => '"' + column + '" = $' + (i + 1))
          .join(", ")}
          WHERE ${primaryKey
            .map(
              (column, i) => '"' + column + '" = $' + (columns.length + i + 1)
            )
            .join(" AND ")}
            `,
        args: [
          ...columns.map((column) => data[column]),
          ...primaryKey.map((column) => data[column]),
        ],
      }
    }
  } else if (message.headers.operation === "delete") {
    if (debug) {
      console.log("deleting", data)
    }
    return {
      sql: `
            DELETE FROM "${schema}"."${table}"
            WHERE ${primaryKey
              .map((column, i) => '"' + column + '" = $' + (i + 1))
              .join(" AND ")}
          `,
      args: [...primaryKey.map((column) => data[column])],
    }
  }

  throw new Error("Unknown operation " + message.headers.operation)
}

function cacheShapeInfo(
  stream: ShapeStream,
  options: SyncShapeToTableOptions
): Statement {
  // @ts-ignore - this is incorrectly marked as private
  const lastOffset = stream.lastOffset

  // store the offset
  console.log("storing offset", lastOffset, "and shapeId", stream.shapeId)

  return {
    sql: `
    INSERT OR REPLACE INTO ${electricShapesTable}
    (tablename, offset, shape_id)
    VALUES
    (?, ?, ?)
    `,
    args: [options.table, lastOffset, stream.shapeId],
  }
}
