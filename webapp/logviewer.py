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
    for coll in db.collections.keys():
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
    coll = db.get_collection(coll_name)
    context['field_names'] = sorted(coll.get_fields())
    context['hidden_fields'] = [
        '_id',
        'client_id',
        'owner_id',
    ]
    filter_field = request.args.get('filter_field', None)
    filter_value = request.args.get('filter_value', None)
    filt = {}
    if filter_field:
        filt[filter_field] = filter_value
    sort_field = request.args.get('s', '')
    page_num = int(request.args.get('p', '0'))
    per_page = 50
    total_count = coll.search(filt).count()
    total_pages = total_count / per_page
    start_index = page_num * per_page
    end_index = start_index + per_page
    if end_index >= total_count:
        has_more = False
        end_index = total_count
    else:
        has_more = True
    url_fmt = '{base_url}?s={sort_field}&p={page}'
    qkwargs = dict(skip=start_index, limit=per_page)
    context.update(dict(
        has_more=has_more,
        page_num=page_num+1,
        per_page=per_page,
        total_pages=total_pages,
        prev_url=url_fmt.format(
            base_url=request.base_url,
            sort_field=sort_field,
            page=page_num-1,
        ),
        next_url=url_fmt.format(
            base_url=request.base_url,
            sort_field=sort_field,
            page=page_num+1,
        ),
    ))

    if sort_field:
        qkwargs['sort_field'] = sort_field
        sort_dir = 'up'
        if sort_field.startswith('+'):
            sort_dir = 'up'
            sort_field = sort_field[1:]
        elif sort_field.startswith('-'):
            sort_dir = 'down'
            sort_field = sort_field[1:]
        context.update(dict(sort_field=sort_field, sort_dir=sort_dir))

    context['entry_iter'] = coll.search(filt, **qkwargs)
    context['collection_name'] = coll_name

    return render_template('log-collection.html', **context)

if __name__ == '__main__':
    app.run()
