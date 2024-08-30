import {
  Container,
  Flex,
  Checkbox,
  Heading,
  Text,
  TextField,
  Link,
} from "@radix-ui/themes"
import { useShape } from "@electric-sql/react"
import { v4 as uuidv4 } from "uuid"
import { useEffect, useState } from 'react'
import { initDB } from '../sqlite'
import { DatabaseAdapter } from '@electric-sql/drivers/wa-sqlite';
import { hookToElectric } from '../electric_sqlite'

type ToDo = {
  id: string
  title: string
  completed: boolean
  created_at: number
}

export default function Index() {

  const [db, setDb] = useState<DatabaseAdapter | null>(null)

  useEffect(() => {
    (async () => {
      console.log("initializing db")
      const db = await initDB()
      
      await hookToElectric(db)

      setDb(db);
      console.log("Initialized")
      const todos = await db.query({ sql: 'SELECT * FROM todos' })
      console.log(todos);

    })();
  }, [])

  if (!db) {
    return <div>initializing sqlite</div>
  }

  return <InnerIndex db={db} />

}

function useTodos({ db }: { db: DatabaseAdapter }): ToDo[] {
  const [todos, setTodos] = useState<ToDo[]>([]);
  const [counter, setCounter] = useState(0)

  useEffect(() => {
    const id = setInterval(() => {
      setCounter((c) => c + 1)
    }, 500)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    db.query({ sql: 'SELECT * FROM todos' }).then((rows) => {
      setTodos(rows.map((row) => {
        return {
          id: row[0] as string,
          title: row[1] as string,
          completed: !!row[2],
          created_at: row[3] as number,
        }
      }))

    })

  }, [counter])

  return todos
}

function InnerIndex({ db }: { db: DatabaseAdapter }) {
  // const { data: todos } = useShape({
  //   url: `http://localhost:3000/v1/shape/todos`,
  // }) as unknown as { data: ToDo[] }

  const todos = useTodos({ db })

  todos.sort((a, b) => a.created_at - b.created_at)



  return (
    <Container size="1">
      <Flex gap="5" mt="5" direction="column">
        <Heading>Electric TODOS</Heading>

        <Flex gap="3" direction="column">
          {todos.map((todo) => {
            return (
              <Flex key={todo.id} gap="2" align="center">
                <Text as="label">
                  <Flex gap="2" align="center">
                    <Checkbox
                      checked={todo.completed}
                      onClick={async () => {
                        console.log(`completed`)
                        await fetch(`http://localhost:3010/todos/${todo.id}`, {
                          method: `PUT`,
                          headers: {
                            "Content-Type": `application/json`,
                          },
                          body: JSON.stringify({
                            completed: !todo.completed,
                          }),
                        })
                      }}
                    />
                    {todo.title}
                  </Flex>
                </Text>
                <Link
                  underline="always"
                  ml="auto"
                  style={{ cursor: `pointer` }}
                  onClick={async () => {
                    console.log(`deleted`)
                    await fetch(`http://localhost:3010/todos/${todo.id}`, {
                      method: `DELETE`,
                    })
                  }}
                >
                  x
                </Link>
              </Flex>
            )
          })}
        </Flex>
        <form
          onSubmit={async (event) => {
            event.preventDefault()
            const id = uuidv4()
            const formData = Object.fromEntries(
              new FormData(event.target as HTMLFormElement)
            )
            const res = await fetch(`http://localhost:3010/todos`, {
              method: `POST`,
              headers: {
                "Content-Type": `application/json`,
              },
              body: JSON.stringify({ id, title: formData.todo }),
            })
            console.log({ res })
          }}
        >
          <TextField.Root type="text" name="todo" placeholder="New Todo" />
        </form>
      </Flex>
    </Container>
  )
}
