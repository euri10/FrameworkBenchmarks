import os
import jinja2
from fastapi import FastAPI, Depends
from operator import attrgetter
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.responses import HTMLResponse, UJSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.responses import Response
from random import randint
import sys

_is_pypy = hasattr(sys, 'pypy_version_info')


engine = create_engine('postgresql://benchmarkdbuser:benchmarkdbpass@tfb-database:5432/hello_world')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
db_session = SessionLocal()


class Fortune(Base):
    __tablename__ = "fortune"
    id = Column(Integer, primary_key=True)
    message = Column(String)


class World(Base):
    __tablename__ = "world"
    id = Column(Integer, primary_key=True)
    randomnumber = Column(Integer)


def get_db(request: Request):
    return request.state.db


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


template = load_fortunes_template()


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


@app.get('/dborm')
def single_database_query_orm(db: Session = Depends(get_db)):
    row_id = randint(1, 10000)
    number = db.query(World.randomnumber).filter(World.id == row_id).one()
    return UJSONResponse({'id': row_id, 'randomNumber': number[0]})


@app.get('/queriesorm')
def multiple_database_queries_orm(queries=None, db: Session = Depends(get_db)):
    num_queries = get_num_queries(queries)
    row_ids = [randint(1, 10000) for _ in range(num_queries)]
    worlds = []
    for row_id in row_ids:
        number = db.query(World.randomnumber).filter(World.id == row_id).one()
        worlds.append({'id': row_id, 'randomNumber': number[0]})
    return worlds


@app.get('/fortunesorm')
def fortunes(db: Session = Depends(get_db)):
    fortunes = list(db.query(Fortune).all())
    fortunes.append(Fortune(id=0, message="Additional fortune added at request time."))
    fortunes.sort(key=attrgetter('message'))
    fortunes = [(fortune.id, fortune.message) for fortune in fortunes]
    content = template.render(fortunes=fortunes)
    return HTMLResponse(content)


@app.get('/updatesorm')
def database_updates_orm(queries = None, db: Session = Depends(get_db)):
    num_queries = get_num_queries(queries)
    updates = [(randint(1, 10000), randint(1, 10000)) for _ in range(num_queries)]
    worlds = []
    for row_id, number in updates:
        world = db.query(World).filter(World.id == row_id).one()
        world.randomnumber = number
        worlds.append({'id': world.id, 'randomNumber': world.randomnumber})
    db.commit()
    return UJSONResponse(worlds)


@app.get('/plaintext')
async def plaintext():
    return PlainTextResponse(b'Hello, world!')
