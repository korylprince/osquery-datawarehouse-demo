import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Direct pod DNS for the first StatefulSet replica via the headless Service.
GARAGE_API = 'http://garage-0.garage-headless.garage.svc.cluster.local:3903'
ADMIN_TOKEN = os.environ['ADMIN_TOKEN']
BUCKET_NAME = 'default'
KEY_NAME = 's3-key'
ENDPOINT_URL = os.environ.get('GARAGE_ENDPOINT', 'http://garage.garage.svc.cluster.local:3900')
CAPACITY_BYTES = int(float(os.environ.get('CAPACITY_BYTES', '10000000000')))
ZONE = os.environ.get('ZONE', 'dc1')

K8S_API = 'https://kubernetes.default.svc'
K8S_CA = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
K8S_TOKEN = open(
    '/var/run/secrets/kubernetes.io/serviceaccount/token',
    'r',
    encoding='utf-8',
).read().strip()
NAMESPACE = open(
    '/var/run/secrets/kubernetes.io/serviceaccount/namespace',
    'r',
    encoding='utf-8',
).read().strip()
# Comma-separated namespaces to replicate garage-s3-credentials to via push mode.
REPLICATION_NAMESPACES = os.environ.get('REPLICATION_NAMESPACES', '')


def request_json(url, method='GET', headers=None, body=None, context=None, allowed_statuses=(200,)):
    encoded = None
    request_headers = {} if headers is None else dict(headers)
    if body is not None:
        encoded = json.dumps(body).encode('utf-8')
        request_headers.setdefault('Content-Type', 'application/json')
    req = urllib.request.Request(url, data=encoded, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(req, context=context, timeout=10) as response:
            payload = response.read().decode('utf-8')
            if payload:
                return response.status, json.loads(payload)
            return response.status, None
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode('utf-8', errors='replace')
        if exc.code in allowed_statuses:
            return exc.code, json.loads(payload) if payload else None
        print(f'HTTP {exc.code} for {method} {url}', file=sys.stderr)
        if payload:
            print(payload, file=sys.stderr)
        raise


def garage_get(endpoint, query=None):
    url = f'{GARAGE_API}/v2/{endpoint}'
    if query:
        url += '?' + urllib.parse.urlencode(query)
    _, data = request_json(
        url,
        headers={'Authorization': f'Bearer {ADMIN_TOKEN}'},
    )
    return data


def garage_post(endpoint, body):
    _, data = request_json(
        f'{GARAGE_API}/v2/{endpoint}',
        method='POST',
        headers={'Authorization': f'Bearer {ADMIN_TOKEN}'},
        body=body,
    )
    return data


def wait_for_garage():
    print('Waiting for garage API...')
    for attempt in range(1, 91):
        try:
            status, _ = request_json(
                f'{GARAGE_API}/v2/GetClusterHealth',
                headers={'Authorization': f'Bearer {ADMIN_TOKEN}'},
                allowed_statuses=(200, 503),
            )
            print(f'Garage API reachable (HTTP {status})')
            return
        except Exception:
            if attempt == 90:
                raise RuntimeError('Garage API not reachable after 90 attempts')
            time.sleep(2)


def wait_for_healthy():
    print('  Waiting for cluster to become healthy...')
    for attempt in range(1, 31):
        status, _ = request_json(
            f'{GARAGE_API}/v2/GetClusterHealth',
            headers={'Authorization': f'Bearer {ADMIN_TOKEN}'},
            allowed_statuses=(200, 503),
        )
        if status == 200:
            print('  Cluster healthy.')
            return
        if attempt < 30:
            time.sleep(2)
    raise RuntimeError('Cluster did not become healthy after layout apply')


def upsert_secret(access_key_id, secret_access_key):
    context = ssl.create_default_context(cafile=K8S_CA)
    headers = {'Authorization': f'Bearer {K8S_TOKEN}'}
    data = {
        'AWS_ACCESS_KEY_ID': base64.b64encode(access_key_id.encode('utf-8')).decode('ascii'),
        'AWS_SECRET_ACCESS_KEY': base64.b64encode(secret_access_key.encode('utf-8')).decode('ascii'),
        'AWS_ENDPOINT_URL': base64.b64encode(ENDPOINT_URL.encode('utf-8')).decode('ascii'),
    }
    annotations = {}
    if REPLICATION_NAMESPACES:
        annotations['replicator.v1.mittwald.de/replicate-to'] = REPLICATION_NAMESPACES
        annotations['replicator.v1.mittwald.de/replication-allowed'] = 'true'
        annotations['replicator.v1.mittwald.de/replication-allowed-namespaces'] = REPLICATION_NAMESPACES
    secret_url = f'{K8S_API}/api/v1/namespaces/{NAMESPACE}/secrets/garage-s3-credentials'
    status, _ = request_json(secret_url, headers=headers, context=context, allowed_statuses=(200, 404))
    if status == 404:
        body = {
            'apiVersion': 'v1',
            'kind': 'Secret',
            'metadata': {
                'name': 'garage-s3-credentials',
                'namespace': NAMESPACE,
                'annotations': annotations,
            },
            'type': 'Opaque',
            'data': data,
        }
        request_json(
            f'{K8S_API}/api/v1/namespaces/{NAMESPACE}/secrets',
            method='POST',
            headers=headers,
            body=body,
            context=context,
        )
        print('  Secret created.')
        return
    patch = {'data': data, 'type': 'Opaque'}
    if annotations:
        patch['metadata'] = {'annotations': annotations}
    request_json(
        secret_url,
        method='PATCH',
        headers={**headers, 'Content-Type': 'application/merge-patch+json'},
        body=patch,
        context=context,
    )
    print('  Secret updated.')


wait_for_garage()

print('Getting cluster status...')
status = garage_get('GetClusterStatus')
active_nodes = [node for node in status['nodes'] if node['isUp']]
if not active_nodes:
    raise RuntimeError('No active Garage nodes found')
node_id = active_nodes[0]['id']
layout_version = status['layoutVersion']
print(f'  Node ID: {node_id}')
print(f'  Layout version: {layout_version}')

print('Checking layout...')
layout = garage_get('GetClusterLayout')
if not layout['roles']:
    print('  No roles assigned. Assigning layout...')
    garage_post(
        'UpdateClusterLayout',
        {
            'roles': [
                {
                    'id': node_id,
                    'zone': ZONE,
                    'capacity': CAPACITY_BYTES,
                    'tags': [],
                }
            ]
        },
    )
    next_version = layout_version + 1
    print(f'  Applying layout version {next_version}...')
    garage_post('ApplyClusterLayout', {'version': next_version})
    print('  Layout applied.')
    wait_for_healthy()
else:
    print(f"  Layout already has {len(layout['roles'])} role(s). Skipping.")

print('Checking buckets...')
buckets = garage_get('ListBuckets')
bucket = next(
    (item for item in buckets if BUCKET_NAME in item.get('globalAliases', [])),
    None,
)
if bucket is None:
    print(f"  Creating bucket '{BUCKET_NAME}'...")
    bucket = garage_post('CreateBucket', {'globalAlias': BUCKET_NAME})
    print(f"  Bucket created: {bucket['id']}")
else:
    print(f"  Bucket '{BUCKET_NAME}' exists: {bucket['id']}")
bucket_id = bucket['id']

print('Checking keys...')
keys = garage_get('ListKeys')
key_summary = next((item for item in keys if item.get('name') == KEY_NAME), None)
if key_summary is None:
    print(f"  Creating key '{KEY_NAME}'...")
    key_info = garage_post('CreateKey', {'name': KEY_NAME})
    print(f"  Key created: {key_info['accessKeyId']}")
else:
    print(f"  Key '{KEY_NAME}' exists: {key_summary['id']}")
    key_info = garage_get(
        'GetKeyInfo',
        {'id': key_summary['id'], 'showSecretKey': 'true'},
    )

access_key_id = key_info['accessKeyId']
secret_access_key = key_info['secretAccessKey']
if not secret_access_key:
    raise RuntimeError('Garage did not return a secret access key')

print('Ensuring bucket permissions...')
garage_post(
    'AllowBucketKey',
    {
        'bucketId': bucket_id,
        'accessKeyId': access_key_id,
        'permissions': {
            'read': True,
            'write': True,
            'owner': True,
        },
    },
)
print('  Permissions granted.')

print('Storing S3 credentials in Secret...')
upsert_secret(access_key_id, secret_access_key)

print('')
print('=== Garage initialization complete ===')
print(f'S3 Endpoint: {ENDPOINT_URL}')
print(f'Bucket: {BUCKET_NAME}')
print(f'Access Key ID: {access_key_id}')
print('Credentials stored in Secret: garage-s3-credentials')
