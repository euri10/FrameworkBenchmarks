import multiprocessing
import os
import sys

_is_pypy = hasattr(sys, "pypy_version_info")
_is_travis = os.environ.get("TRAVIS") == "true"

workers = multiprocessing.cpu_count()
if _is_travis:
    workers = 2

bind = "0.0.0.0:8080"
keepalive = 120
errorlog = "-"
pidfile = "/tmp/fastapi.pid"
loglevel = "error"

if _is_pypy:
    worker_class = "uvicorn.workers.UvicornH11Worker"
else:
    worker_class = "uvicorn.workers.UvicornWorker"
