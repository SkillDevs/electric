import { DatabaseAdapter } from "@electric-sql/drivers/wa-sqlite"
import { QualifiedTablename, tableExistsStmt } from "./util"

export const electricMetaTable = "_electric_meta"
export const electricShapesTable = "_electric_shapes"

export async function initElectricSchema(db: DatabaseAdapter) {
  const res = await db.query(
    tableExistsStmt(new QualifiedTablename("main", electricMetaTable))
  )

  let version: number = -1
  if (res.length > 0) {
    const rows = await db.query({
      sql: `SELECT version FROM ${electricMetaTable}`,
    })
    version = rows[0].version as number
  }

  if (version === -1) {
    console.log("Initializing electric schema")
    await _migration1(db)
  } else if (version !== 1) {
    throw new Error("Unsupported electric schema version " + version)
  }
}

async function _migration1(db: DatabaseAdapter) {
  await db.runInTransaction(
    {
      sql: `CREATE TABLE ${electricMetaTable} (
        version INTEGER NOT NULL
    )`,
    },
    {
      sql: `CREATE TABLE ${electricShapesTable} (
        tablename TEXT NOT NULL,
        shape_id TEXT NOT NULL,
        offset INTEGER NOT NULL
    )`,
    },

    {
      sql: `INSERT INTO ${electricMetaTable} (version) VALUES (1)`,
    }
  )
}
