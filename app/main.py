from flask import Flask, jsonify, request, redirect
from logging import getLogger

import boilerplate

from hseling_api_template.process import process_data


ALLOWED_EXTENSIONS = ['txt']


log = getLogger(__name__)


app = Flask(__name__)
app.config.update(
    CELERY_BROKER_URL=boilerplate.CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND=boilerplate.CELERY_RESULT_BACKEND
)
celery = boilerplate.make_celery(app)


@celery.task
def process_task(file_ids_list=None):
    files_to_process = boilerplate.list_files(recursive=True, prefix=boilerplate.UPLOAD_PREFIX)
    if file_ids_list:
        files_to_process = [boilerplate.UPLOAD_PREFIX + file_id for file_id in file_ids_list
                            if (boilerplate.UPLOAD_PREFIX + file_id) in files_to_process]
    data_to_process = {file_id[len(boilerplate.UPLOAD_PREFIX):]: \
                       boilerplate.get_file(file_id) for file_id in files_to_process}
    processed_file_ids = list()
    print(data_to_process)
    for processed_file_id, contents in process_data(data_to_process):
        processed_file_ids.append(boilerplate.add_processed_file(processed_file_id, contents))
    return processed_file_ids


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": boilerplate.ERROR_NO_FILE_PART})
        upload_file = request.files['file']
        if upload_file.filename == '':
            return jsonify({"error": boilerplate.ERROR_NO_SELECTED_FILE})
        if upload_file and boilerplate.allowed_file(upload_file.filename, allowed_extensions=ALLOWED_EXTENSIONS):
            return jsonify(boilerplate.save_file(upload_file))
    return boilerplate.get_upload_form()


@app.route('/files')
def list_files_endpoint():
    return jsonify({'file_ids': boilerplate.list_files(recursive=True)})


@app.route('/process')
@app.route("/process/<file_ids>")
def process_endpoint(file_ids=None):
    file_ids_list = file_ids and file_ids.split(",")
    task = process_task.delay(file_ids_list)
    return jsonify({"task_id": str(task)})


@app.route("/status/<task_id>")
def status_endpoint(task_id):
    return jsonify(boilerplate.get_task_status(task_id))


def get_endpoints(ctx):
    def endpoint(name, description, active=True):
        return {
            "name": name,
            "description": description,
            "active": active
        }

    all_endpoints = [
        endpoint("root", boilerplate.ENDPOINT_ROOT),
        endpoint("scrap", boilerplate.ENDPOINT_SCRAP, not ctx["restricted_mode"]),
        endpoint("upload", boilerplate.ENDPOINT_UPLOAD),
        endpoint("process", boilerplate.ENDPOINT_PROCESS),
        endpoint("query", boilerplate.ENDPOINT_QUERY),
        endpoint("status", boilerplate.ENDPOINT_STATUS)
    ]

    return {ep["name"]: ep for ep in all_endpoints if ep}


@app.route("/")
def main():
    ctx = {"restricted_mode": RESTRICTED_MODE}
    return jsonify({"endpoints": get_endpoints(ctx)})


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=80)


__all__ = [app, celery]
