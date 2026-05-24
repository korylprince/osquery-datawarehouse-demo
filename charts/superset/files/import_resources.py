#!/usr/bin/env python3
"""Import Superset resources (datasets, charts, dashboards) from JSON definitions.

Designed to run as an Argo CD post-sync hook job inside the cluster.
Connects to Superset via internal service: http://superset:8088
"""
import json
import urllib.request
import urllib.error
import http.cookiejar
import time
import os
import secrets
import string

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088")
USERNAME = os.environ.get("SUPERSET_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")
RESOURCES_FILE = os.environ.get("RESOURCES_FILE", "/resources/resources.json")
MAX_RETRIES = 30
RETRY_DELAY = 2


def auth():
    """Login and get JWT + CSRF token. Returns (get_fn, mutate_fn)."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    data = json.dumps({"username": USERNAME, "password": PASSWORD, "provider": "db"}).encode()
    req = urllib.request.Request(
        f"{BASE}/api/v1/security/login",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = opener.open(req)
    token = json.loads(resp.read().decode())["access_token"]
    auth_header = f"Bearer {token}"

    req2 = urllib.request.Request(
        f"{BASE}/api/v1/security/csrf_token/",
        headers={"Authorization": auth_header},
    )
    resp2 = opener.open(req2)
    csrf = json.loads(resp2.read().decode())["result"]

    def get(path):
        req = urllib.request.Request(
            f"{BASE}{path}",
            headers={"Authorization": auth_header},
        )
        resp = opener.open(req)
        return json.loads(resp.read().decode())

    def mutate(path, body, method="POST"):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{BASE}{path}",
            data=data,
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
                "X-CSRFToken": csrf,
            },
            method=method,
        )
        resp = opener.open(req)
        return json.loads(resp.read().decode())

    return get, mutate


def wait_for_superset(get_fn):
    """Wait until Superset API is responsive."""
    for attempt in range(MAX_RETRIES):
        try:
            get_fn("/api/v1/security/csrf_token/")
            print("Superset is ready.")
            return True
        except Exception:
            if attempt < MAX_RETRIES - 1:
                print(f"Waiting for Superset... ({attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"Superset not ready after {MAX_RETRIES} attempts")
    return False


def find_database(get_fn, db_name):
    """Find database by name, return its id."""
    dbs = get_fn("/api/v1/database/")
    for db in dbs.get("result", []):
        if db["database_name"] == db_name:
            return db["id"]
    return None


def find_existing_dataset(get_fn, table_name, schema):
    """Check if a dataset already exists. Returns id or None."""
    existing = get_fn("/api/v1/dataset/")
    for item in existing.get("result", []):
        if item.get("table_name") == table_name and item.get("schema") == schema:
            return item["id"]
    return None


def create_datasets(get_fn, mutate_fn, datasets):
    """Create or update datasets. Returns list of (definition, id)."""
    results = []
    for ds in datasets:
        db_id = find_database(get_fn, ds["database_name"])
        if db_id is None:
            print(f"  ERROR: Database '{ds['database_name']}' not found. Skipping '{ds['table_name']}'.")
            continue

        table_name = ds["table_name"]
        schema = ds.get("schema", "default")

        existing_id = find_existing_dataset(get_fn, table_name, schema)
        if existing_id:
            print(f"  Dataset '{table_name}' exists (id={existing_id}). Updating if needed.")
            if ds.get("main_dttm_col"):
                try:
                    mutate_fn(f"/api/v1/dataset/{existing_id}", {"main_dttm_col": ds["main_dttm_col"]}, method="PUT")
                except urllib.error.HTTPError as e:
                    body = e.read().decode() if hasattr(e, "read") else str(e)
                    print(f"    Warning: could not update main_dttm_col: {body[:100]}")
            results.append((ds, existing_id))
            continue

        payload = {
            "database": db_id,
            "schema": schema,
            "table_name": table_name,
            "sql": ds.get("sql"),
        }
        resp = mutate_fn("/api/v1/dataset/", payload, method="POST")
        created_id = resp["id"]

        if ds.get("main_dttm_col"):
            try:
                mutate_fn(f"/api/v1/dataset/{created_id}", {"main_dttm_col": ds["main_dttm_col"]}, method="PUT")
            except urllib.error.HTTPError as e:
                body = e.read().decode() if hasattr(e, "read") else str(e)
                print(f"    Warning: could not set main_dttm_col: {body[:100]}")

        print(f"  Created dataset '{table_name}' (id={created_id})")
        results.append((ds, created_id))

    return results


def update_params_datasource(params_str, dataset_id):
    """Replace dataset ID references in params JSON string."""
    params = json.loads(params_str)
    datasource = params.get("datasource", "")
    if datasource.endswith("__table"):
        params["datasource"] = f"{dataset_id}__table"
    return json.dumps(params)


def find_existing_chart(get_fn, slice_name):
    """Check if a chart already exists. Returns (id, uuid) or None."""
    existing = get_fn("/api/v1/chart/")
    for item in existing.get("result", []):
        if item.get("slice_name") == slice_name:
            return item["id"], item.get("uuid")
    return None


def create_charts(get_fn, mutate_fn, charts, dataset_results):
    """Create or update charts. Returns list of (definition, id, uuid)."""
    ds_id = dataset_results[0][1] if dataset_results else None

    results = []
    for ch in charts:
        slice_name = ch["slice_name"]
        existing = find_existing_chart(get_fn, slice_name)

        params = update_params_datasource(ch["params"], ds_id)

        if existing:
            existing_id, existing_uuid = existing
            print(f"  Chart '{slice_name}' exists (id={existing_id}). Updating.")
            mutate_fn(f"/api/v1/chart/{existing_id}", {"params": params}, method="PUT")
            results.append((ch, existing_id, existing_uuid))
            continue

        payload = {
            "slice_name": slice_name,
            "viz_type": ch["viz_type"],
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": params,
        }
        resp = mutate_fn("/api/v1/chart/", payload, method="POST")
        created_id = resp["id"]
        # Get the UUID from the created chart detail
        detail = get_fn(f"/api/v1/chart/{created_id}")
        created_uuid = detail["result"].get("uuid")
        print(f"  Created chart '{slice_name}' (id={created_id})")
        results.append((ch, created_id, created_uuid))

    return results


def random_id(length=20):
    """Generate a random ID similar to Superset's nanoid format."""
    alphabet = string.ascii_letters + string.digits + "_-"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def update_position_json(position_json_str, old_refs, new_results):
    """Update chart IDs, UUIDs, and regenerate node keys in dashboard position_json."""
    pos = json.loads(position_json_str)
    id_map = {old_id: new_id for (_, old_id, _), (_, new_id, _) in zip(old_refs, new_results)}
    uuid_map = {old_uuid: new_uuid for (_, _, old_uuid), (_, _, new_uuid) in zip(old_refs, new_results) if old_uuid and new_uuid}

    # Find old chart node IDs and their slice names
    old_chart_nodes = {}
    for key, node in pos.items():
        if isinstance(node, dict) and node.get("type") == "CHART":
            old_chart_nodes[key] = node.get("meta", {}).get("sliceName")

    # Find old ROW IDs
    old_row_ids = []
    for key, node in pos.items():
        if isinstance(node, dict) and node.get("type") == "ROW":
            old_row_ids.append(key)

    # Build mapping from old node IDs to new node IDs
    node_id_map = {}
    for old_key, slice_name in old_chart_nodes.items():
        new_key = f"CHART-{random_id()}"
        node_id_map[old_key] = new_key

    row_id_map = {}
    for old_row in old_row_ids:
        new_row = f"ROW-{random_id()}"
        row_id_map[old_row] = new_row

    # Update all nodes: regenerate chart meta IDs, update chartId/uuid, remap row IDs
    for key, node in pos.items():
        if isinstance(node, dict) and node.get("type") == "CHART":
            meta = node.get("meta", {})
            chart_id = meta.get("chartId")
            if chart_id in id_map:
                meta["chartId"] = id_map[chart_id]
            chart_uuid = meta.get("uuid")
            if chart_uuid in uuid_map:
                meta["uuid"] = uuid_map[chart_uuid]
            node["meta"] = meta
            # Update parents to use new ROW IDs
            parents = node.get("parents", [])
            node["parents"] = [row_id_map.get(p, p) for p in parents]

    # Remap all children/parents references to new node IDs
    for key, node in pos.items():
        if isinstance(node, dict):
            children = node.get("children", [])
            node["children"] = [
                node_id_map.get(c, row_id_map.get(c, c)) for c in children
            ]
            parents = node.get("parents", [])
            node["parents"] = [
                node_id_map.get(p, row_id_map.get(p, p)) for p in parents
            ]

    # Rename keys: move old keys to new keys, remove old keys
    for old_key, new_key in {**node_id_map, **row_id_map}.items():
        if old_key in pos:
            pos[new_key] = pos.pop(old_key)
            pos[new_key]["id"] = new_key

    return json.dumps(pos)


def update_json_metadata(json_metadata_str, old_refs, new_results):
    """Remap chart IDs in dashboard json_metadata (chart_configuration, crossFilters, chartsInScope)."""
    meta = json.loads(json_metadata_str)
    id_map = {old_id: new_id for (_, old_id, _), (_, new_id, _) in zip(old_refs, new_results)}

    # Remap chart_configuration: update keys, id values, and crossFilters.chartsInScope
    if "chart_configuration" in meta:
        new_cfg = {}
        for key, val in meta["chart_configuration"].items():
            try:
                int_key = int(key)
            except ValueError:
                int_key = None
            new_key = str(id_map.get(int_key, int_key or key)) if int_key else key
            cfg_copy = dict(val)
            if "id" in cfg_copy and cfg_copy["id"] in id_map:
                cfg_copy["id"] = id_map[cfg_copy["id"]]
            xf = cfg_copy.get("crossFilters")
            if isinstance(xf, dict) and "chartsInScope" in xf:
                xf["chartsInScope"] = [id_map.get(c, c) for c in xf["chartsInScope"]]
            cfg_copy["crossFilters"] = xf
            new_cfg[new_key] = cfg_copy
        meta["chart_configuration"] = new_cfg

    # Remap global_chart_configuration.chartsInScope
    gcfg = meta.get("global_chart_configuration")
    if isinstance(gcfg, dict) and "chartsInScope" in gcfg:
        gcfg["chartsInScope"] = [id_map.get(c, c) for c in gcfg["chartsInScope"]]

    return json.dumps(meta)


def find_existing_dashboard(get_fn, title):
    """Check if a dashboard already exists. Returns id or None."""
    existing = get_fn("/api/v1/dashboard/")
    for item in existing.get("result", []):
        if item.get("dashboard_title") == title:
            return item["id"]
    return None


# Original chart IDs/UUIDs from the export
OLD_CHART_REFS = [
    ({} , 1, "fd863cfb-2e54-4d4a-85f2-46f626f6646f"),
    ({} , 2, "984d7848-515c-4ed2-919c-69f20dd8e0d0"),
]


def create_dashboards(get_fn, mutate_fn, dashboards, chart_results):
    """Create or update dashboards, then link charts to dashboard."""
    for db_def in dashboards:
        title = db_def["dashboard_title"]
        publish = db_def.get("published", True)
        existing_id = find_existing_dashboard(get_fn, title)

        updated_pos = update_position_json(
            db_def["position_json"],
            OLD_CHART_REFS,
            chart_results,
        )

        updated_meta = update_json_metadata(
            db_def.get("json_metadata", "{}"),
            OLD_CHART_REFS,
            chart_results,
        )

        payload = {
            "dashboard_title": title,
            "position_json": updated_pos,
            "json_metadata": updated_meta,
            "css": db_def.get("css", ""),
            "published": publish,
        }

        if existing_id:
            print(f"  Dashboard '{title}' exists (id={existing_id}). Updating.")
            mutate_fn(f"/api/v1/dashboard/{existing_id}", payload, method="PUT")
            dash_id = existing_id
        else:
            resp = mutate_fn("/api/v1/dashboard/", payload, method="POST")
            dash_id = resp["id"]
            print(f"  Created dashboard '{title}' (id={dash_id})")

        # Link charts to dashboard (establishes dashboard-chart relationship)
        for _, chart_id, _ in chart_results:
            mutate_fn(f"/api/v1/chart/{chart_id}", {"dashboards": [dash_id]}, method="PUT")
        print(f"  Linked {len(chart_results)} chart(s) to dashboard.")


def main():
    print(f"Loading resources from {RESOURCES_FILE}...")
    with open(RESOURCES_FILE) as f:
        resources = json.load(f)

    get_fn, mutate_fn = auth()
    wait_for_superset(get_fn)

    print("\n--- Datasets ---")
    dataset_results = create_datasets(get_fn, mutate_fn, resources.get("datasets", []))

    print("\n--- Charts ---")
    chart_results = create_charts(get_fn, mutate_fn, resources.get("charts", []), dataset_results)

    print("\n--- Dashboards ---")
    create_dashboards(get_fn, mutate_fn, resources.get("dashboards", []), chart_results)

    print("\nDone. All Superset resources imported.")


if __name__ == "__main__":
    main()
