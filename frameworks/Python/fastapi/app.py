import asyncio
import asyncpg
import os
import jinja2
from fastapi import FastAPI, Depends
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Boolean, Column, Integer, create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.responses import HTMLResponse, UJSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.responses import Response
from random import randint
import sys
from operator import itemgetter

_is_pypy = hasattr(sys, 'pypy_version_info')


READ_ROW_SQL = 'SELECT "randomnumber" FROM "world" WHERE id = $1'
WRITE_ROW_SQL = 'UPDATE "world" SET "randomnumber"=$1 WHERE id=$2'
ADDITIONAL_ROW = [0, 'Additional fortune added at request time.']

# ============ ORM stuff =====================

engine = create_engine('postgresql://benchmarkdbuser:benchmarkdbpass@tfb-database:5432/hello_world')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
db_session = SessionLocal()


class World(Base):
    __tablename__ = "world"
    id = Column(Integer, primary_key=True)
    randomnumber = Column(Integer)


def get_db(request: Request):
    return request.state.db

# ============================================


async def setup_database():
    global connection_pool
    connection_pool = await asyncpg.create_pool(
        user=os.getenv('PGUSER', 'benchmarkdbuser'),
        password=os.getenv('PGPASS', 'benchmarkdbpass'),
        database='hello_world',
        host='tfb-database',
        port=5432
    )


def load_fortunes_template():
    path = os.path.join('templates', 'fortune.html')
    with open(path, 'r') as template_file:
        template_text = template_file.read()
        return jinja2.Template(template_text)


def get_num_queries(queries):
    try:
        query_count = int(queries)
    except (ValueError, TypeError):
        return 1

    if query_count < 1:
        return 1
    if query_count > 500:
        return 500
    return query_count


connection_pool = None
sort_fortunes_key = itemgetter(1)
template = load_fortunes_template()
loop = asyncio.get_event_loop()
loop.run_until_complete(setup_database())


app = FastAPI()


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    response = Response("Internal server error", status_code=500)
    try:
        request.state.db = SessionLocal()
        response = await call_next(request)
    finally:
        request.state.db.close()
    return response


@app.get('/json')
async def json_serialization():
    return UJSONResponse({'message': 'Hello, world!'})


@app.get('/db')
async def single_database_query():
    row_id = randint(1, 10000)

    async with connection_pool.acquire() as connection:
        number = await connection.fetchval(READ_ROW_SQL, row_id)

    return UJSONResponse({'id': row_id, 'randomNumber': number})


@app.get('/dborm')
def single_database_query_orm(db: Session = Depends(get_db)):
    row_id = randint(1, 10000)
    number = db.query(World.randomnumber).filter(World.id == row_id).one()
    return UJSONResponse({'id': row_id, 'randomNumber': number[0]})


@app.get('/queries')
async def multiple_database_queries(queries = None):

    num_queries = get_num_queries(queries)
    row_ids = [randint(1, 10000) for _ in range(num_queries)]
    worlds = []

    async with connection_pool.acquire() as connection:
        statement = await connection.prepare(READ_ROW_SQL)
        for row_id in row_ids:
            number = await statement.fetchval(row_id)
            worlds.append({'id': row_id, 'randomNumber': number})

    return UJSONResponse(worlds)


@app.get('/queriesorm')
def multiple_database_queries(queries=None, db: Session = Depends(get_db)):
    num_queries = get_num_queries(queries)
    row_ids = [randint(1, 10000) for _ in range(num_queries)]
    worlds = []
    for row_id in row_ids:
        number = db.query(World.randomnumber).filter(World.id == row_id).one()
        worlds.append({'id': row_id, 'randomNumber': number[0]})
    return worlds


@app.get('/fortunes')
async def fortunes():
    async with connection_pool.acquire() as connection:
        fortunes = await connection.fetch('SELECT * FROM Fortune')

    fortunes.append(ADDITIONAL_ROW)
    fortunes.sort(key=sort_fortunes_key)
    content = template.render(fortunes=fortunes)
    return HTMLResponse(content)


@app.get('/updates')
async def database_updates(queries = None):
    num_queries = get_num_queries(queries)
    updates = [(randint(1, 10000), randint(1, 10000)) for _ in range(num_queries)]
    worlds = [{'id': row_id, 'randomNumber': number} for row_id, number in updates]

    async with connection_pool.acquire() as connection:
        statement = await connection.prepare(READ_ROW_SQL)
        for row_id, number in updates:
            await statement.fetchval(row_id)
        await connection.executemany(WRITE_ROW_SQL, updates)

    return UJSONResponse(worlds)


@app.get('/plaintext')
async def plaintext():
    return PlainTextResponse(b'Hello, world!')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('app:app', reload=True)
