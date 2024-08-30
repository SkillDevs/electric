import {
  DatabaseAdapter,
  ElectricDatabase,
} from "@electric-sql/drivers/wa-sqlite"

export async function initDB() {
  const dbName = "myDB1"
  const rawDb = await ElectricDatabase.init(dbName)

  const db = new DatabaseAdapter(rawDb)

  const rows = await db.query({ sql: "PRAGMA user_version" })
  const userVersion = rows[0]["user_version"] as number

  console.log("DB VERSION", userVersion)

  if (userVersion === 0) {
    await db.run({
      sql: `CREATE TABLE todos (
                id TEXT PRIMARY KEY NOT NULL, 
                title TEXT NOT NULL, 
                completed INTEGER NOT NULL,
				        created_at INTEGER NOT NULL)`,
    })

    // 						for (let i = 0; i < 1; i++) {
    // await sqlite3.exec(
    //     db, `INSERT INTO todos(id, title, completed) VALUES (${i}, 'hola',
    //     0)`);
    // 						}

    await db.run({ sql: `PRAGMA user_version = 1` })
  }

  return db
}
