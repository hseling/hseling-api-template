from os import environ, listdir, path, walk
import hashlib
from io import BytesIO, SEEK_END, SEEK_SET
from uuid import uuid4

from celery import Celery, result
from werkzeug.utils import secure_filename

from mysql.connector import connect


CELERY_BROKER_URL = environ["CELERY_BROKER_URL"]
CELERY_RESULT_BACKEND = environ["CELERY_RESULT_BACKEND"]

MYSQL_HOST = environ["MYSQL_HOST"]
MYSQL_USER = environ["MYSQL_USER"]
MYSQL_PASSWORD = environ["MYSQL_PASSWORD"]
MYSQL_DATABASE = environ["MYSQL_DATABASE"]

ALLOWED_EXTENSIONS = ['txt', 'xml']
UPLOAD_PREFIX = 'upload/'
PATH_TO_DATA = "/data/"
PROCESSED_PREFIX = 'processed/'

ERROR_NO_FILE_PART = "ERROR_NO_FILE_PART"
ERROR_NO_SELECTED_FILE = "ERROR_NO_SELECTED_FILE"
ERROR_NO_SUCH_FILE = "ERROR_NO_SUCH_FILE"
ERROR_NO_QUERY_TYPE_SPECIFIED = "ERROR_NO_QUERY_TYPE_SPECIFIED"

ENDPOINT_ROOT = "ENDPOINT_ROOT"
ENDPOINT_SCRAP = "ENDPOINT_SCRAP"
ENDPOINT_UPLOAD = "ENDPOINT_UPLOAD"
ENDPOINT_PROCESS = "ENDPOINT_PROCESS"
ENDPOINT_STATUS = "ENDPOINT_STATUS"
ENDPOINT_QUERY = "ENDPOINT_QUERY"


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


def get_mysql_connection():
    mysqlClient = connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                          host=MYSQL_HOST,
                          database=MYSQL_DATABASE)
    return mysqlClient

def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        while True:
            buf = f.read(4096) # 128 is smaller than the typical filesystem block
            if not buf:
                break
            d.update(buf)
        return d.hexdigest()

def put_file(filename, contents, contents_length=None):
    if not isinstance(contents, BytesIO):
        if isinstance(contents, str):
            contents_length = len(contents)
            contents = bytes(contents, encoding='utf-8')
        else:
            contents = bytes(contents)
        contents = BytesIO(contents)
        if not contents_length:
            contents.seek(SEEK_END)
            contents_length = contents.tell()
            contents.seek(SEEK_SET)
    with open(PATH_TO_DATA + filename, 'w+b') as f:
        f.write(contents.read())
    return md5sum(PATH_TO_DATA + filename)

def get_file(filename):
    with open(PATH_TO_DATA + filename) as f:
        return f.read()

def list_files(prefix = None, recursive=True):
    if recursive:
        if prefix is not None:
            return list(path.join(r,file)[len(PATH_TO_DATA):] for r,d,f in walk(PATH_TO_DATA + prefix) for file in f)
        else:
            return list(path.join(r,file)[len(PATH_TO_DATA):] for r,d,f in walk(PATH_TO_DATA) for file in f)
    else:
        if prefix is not None:
            return list(path.join(prefix, f) for f in listdir(PATH_TO_DATA + prefix) if path.isfile(PATH_TO_DATA + prefix + f))
        else:
            return list(f for f in listdir(PATH_TO_DATA + prefix) if path.isfile(PATH_TO_DATA + prefix + f))

def allowed_file(filename, allowed_extensions=None):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() \
           in (allowed_extensions or ALLOWED_EXTENSIONS)


def get_upload_form():
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <p><input type=file name=file>
         <input type=submit value=Upload>
    </form>
    '''


def get_task_status(task_id):
    task = result.AsyncResult(str(task_id))
    if isinstance(task.result, BaseException):
        task_result = str(task.result)
    else:
        task_result = task.result
    return {
        "task_id": str(task.id),
        "ready": task.ready(),
        "status": task.status,
        "result": task_result,
        "error": str(task.traceback)
    }


def save_file(upload_file):
    filename = UPLOAD_PREFIX + secure_filename(upload_file.filename)
    file_bytes = BytesIO()
    upload_file.save(file_bytes)
    file_size = file_bytes.tell()
    file_bytes.seek(SEEK_SET)
    put_file(filename, file_bytes, file_size)
    return {
        'file_id': filename,
        'file_size': file_size
    }


def add_processed_file(processed_file_id, contents, extension=None):
    if not processed_file_id:
        processed_file_id = str(uuid4())
    if extension:
        filename = PROCESSED_PREFIX + processed_file_id + ("." + extension)
    else:
        filename = ""
    put_file(filename, contents)
    return filename
