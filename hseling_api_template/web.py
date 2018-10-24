from os import environ
from celery import Celery, result
from flask import Flask, jsonify
from logging import getLogger

from hseling_api_template.process import process_data


log = getLogger(__name__)


CELERY_BROKER_URL = environ["CELERY_BROKER_URL"]
CELERY_RESULT_BACKEND = environ["CELERY_RESULT_BACKEND"]

MINIO_URL = environ["MINIO_URL"]
MINIO_ACCESS_KEY = environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = environ["MINIO_SECRET_KEY"]

RESTRICTED_MODE = environ["RESTRICTED_MODE"]


def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


app = Flask(__name__)
app.config.update(
    CELERY_BROKER_URL=CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND=CELERY_RESULT_BACKEND
)
celery = make_celery(app)


@celery.task
def process_task(file_ids_list=None):
    if file_ids_list:
        return process_data(*file_ids_list)
    return None


@app.route("/process/<file_ids>")
def process_endpoint(file_ids):
    file_ids_list = file_ids.split(",")
    task = process_task.delay(file_ids_list)
    return jsonify({"task_id": str(task)})


@app.route("/status/<task_id>")
def status_endpoint(task_id):
    task = result.AsyncResult(task_id)
    return jsonify({
        "task_id": task.id,
        "ready": task.ready(),
        "status": task.status,
        "result": task.result,
        "error": task.traceback
    })


def get_endpoints(ctx):
    def endpoint(name, description, active=True):
        return {
            "name": name,
            "description": description,
            "active": active
        }

    all_endpoints = [
        endpoint("root", "Default root route"),
        endpoint("scrap", "Scrap web-site", not ctx["restricted_mode"]),
        endpoint("process", "Process data", False),
        endpoint("query", "Query data", False)
    ]

    return {ep["name"]: ep for ep in all_endpoints if ep}


@app.route("/")
def main():
    ctx = {"restricted_mode": RESTRICTED_MODE}
    return jsonify({"endpoints": get_endpoints(ctx)})


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=80)
