import { Offset, ShapeStream } from "@electric-sql/client"
import { DatabaseAdapter } from "@electric-sql/drivers/wa-sqlite"
import { syncShapeToTable, TableStreamApi } from "./sync_to_table"
import {
  electricMetaTable,
  electricShapesTable,
  initElectricSchema,
} from "./schema"

export async function hookToElectric(db: DatabaseAdapter) {
  await initElectricSchema(db)

  const sqliteTables = ["todos"]

  const streams: Array<{
    stream: ShapeStream
    aborter: AbortController
  }> = []

  const tableToApi: Map<string, TableStreamApi> = new Map()

  const baseUrl = "http://localhost:3000/v1/shape"

  for (const sqliteTable of sqliteTables) {
    const cachedShapeInfo = await getCachedShapeInfo(db, sqliteTable)

    console.log(
      "Start shape stream for table",
      sqliteTable,
      "with offset",
      cachedShapeInfo?.offset ?? "-1",
      "and shapeId",
      cachedShapeInfo?.shapeId
    )

    const tableStreamApi = await syncShapeToTable(streams, db, {
      url: `${baseUrl}/${sqliteTable}`,
      table: sqliteTable,
      primaryKey: ["id"],
      offset: cachedShapeInfo?.offset ?? "-1",
      shapeId: cachedShapeInfo?.shapeId,
      mapColumns: (message) => {
        const data = message.value

        const newData = Object.fromEntries(
          Object.entries(data).map(([k, v]) => {
            let newValue = v

            if (typeof v == "boolean") {
              newValue = v ? 1 : 0
            }

            if (k == "created_at") {
              newValue = Date.parse(v as string)
            }

            return [k, newValue]
          })
        )

        // console.log(data, newData)

        return newData
      },
    })

    tableToApi.set(sqliteTable, tableStreamApi)
  }

  return () => {
    for (const { stream, aborter } of streams) {
      stream.unsubscribeAll()
      aborter.abort()
    }
  }
}

type TableShapeInfo = {
  offset: Offset
  shapeId: string
}

async function getCachedShapeInfo(
  db: DatabaseAdapter,
  table: string
): Promise<TableShapeInfo | undefined> {
  const rows = await db.query({
    sql: `SELECT * FROM ${electricShapesTable} WHERE tablename = ?`,
    args: [table],
  })

  if (rows.length === 0) {
    return undefined
  }

  return {
    offset: rows[0].offset as Offset,
    shapeId: rows[0].shape_id as string,
  }
}
