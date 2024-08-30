import { ShapeStream } from "@electric-sql/client"
import { DatabaseAdapter } from "@electric-sql/drivers/wa-sqlite"
import { syncShapeToTable, TableStreamApi } from "./sync_to_table"

export async function hookToElectric(db: DatabaseAdapter) {
  const sqliteTables = ["todos"]

  const streams: Array<{
    stream: ShapeStream
    aborter: AbortController
  }> = []

  const tableToApi: Map<string, TableStreamApi> = new Map()

  const baseUrl = "http://localhost:3000/v1/shape"

  for (const sqliteTable of sqliteTables) {
    const tableStreamApi = await syncShapeToTable(streams, db, {
      url: `${baseUrl}/${sqliteTable}`,
      table: sqliteTable,
      primaryKey: ["id"],
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

        console.log(data, newData)

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
