from flask import Flask, request, render_template, g, url_for
from werkzeug.local import LocalProxy

from s3logparse.main import build_config
from s3logparse.backends import mongo_storage

BACKEND_CONFIG = build_config()
DEBUG = True

app = Flask(__name__)
app.config.from_object(__name__)

def build_backend():
    return mongo_storage.MongoStorage(config=BACKEND_CONFIG)

def get_db():
    backend = getattr(g, '_db', None)
    if backend is None:
        backend = g._db = build_backend()
        #backend._context_lock.acquire()
        #backend.open()
    return backend

@app.teardown_appcontext
def teardown_db(exception):
    backend = getattr(g, '_db', None)
    if backend is not None:
        #backend._context_lock.release()
        backend.close()
def slugify(s):
    for c in ' ./':
        s = '-'.join(s.split(c))
    return s

def get_log_collections():
    c = getattr(g, '_log_collections', None)
    if c is not None:
        return c
    c = {}
    db = get_db()
    for coll in db.get_log_collections():
        slug = slugify(coll)
        c[slug] = dict(
            name=coll,
            slug=slug,
            url=url_for('log_collection', slug=slug),
        )
    g._log_collections = c
    return c

def get_site_context(context=None):
    if context is None:
        context = {}
    context['log_collections'] = get_log_collections()
    return context


@app.route('/')
def home():
    context = get_site_context()
    return render_template('home.html', **context)

@app.route('/log_collection/<slug>')
def log_collection(slug):
    context = get_site_context()
    coll_name = context['log_collections'][slug]['name']
    db = get_db()
    context['field_names'] = sorted(db.get_fields(coll_name))
    context['hidden_fields'] = [
        '_id',
        'client_id',
        'owner_id',
    ]
    sort_field = request.args.get('s', '')
    page_num = int(request.args.get('p', '0'))
    per_page = 50
    qkwargs = dict(skip=page_num*per_page, limit=per_page)
    if sort_field:
        qkwargs['sort_field'] = sort_field
    context['entry_iter'] = db.get_all_entries(coll_name, **qkwargs)
    return render_template('log-collection.html', **context)

if __name__ == '__main__':
    app.run()
