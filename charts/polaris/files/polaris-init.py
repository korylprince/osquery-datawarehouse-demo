"""Initialize the Polaris catalog used by Flink and Trino.

Creates the warehouse catalog with S3/Garage storage configuration,
sets up catalog roles (catalog_admin, flink_admin), grants access
to the service_admin principal role, and creates the 'default' Iceberg
namespace.

Runs as an ArgoCD Sync hook Job after the Polaris deployment is ready.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

POLARIS_HOST = os.environ.get(
    "POLARIS_HOST", "http://polaris.polaris.svc.cluster.local:8181"
)
CLIENT_ID = os.environ["POLARIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

CATALOG_NAME = "warehouse"
BASE_LOCATION = "s3://default/warehouse/"
S3_ENDPOINT = "http://garage.garage.svc.cluster.local:3900"
S3_REGION = "garage"

# Catalog roles to create and grant CATALOG_MANAGE_CONTENT
CATALOG_ROLES = ["catalog_admin", "flink_admin"]


def request_json(
    url,
    method="GET",
    headers=None,
    body=None,
    content_type="application/json",
    allowed_statuses=(200,),
):
    encoded = None
    request_headers = {} if headers is None else dict(headers)
    if body is not None:
        if content_type == "application/x-www-form-urlencoded":
            encoded = body.encode("utf-8")
        else:
            encoded = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", content_type)
    req = urllib.request.Request(
        url, data=encoded, headers=request_headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = response.read().decode("utf-8")
            if payload:
                return response.status, json.loads(payload)
            return response.status, None
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        if exc.code in allowed_statuses:
            return exc.code, json.loads(payload) if payload else None
        print(f"HTTP {exc.code} for {method} {url}", file=sys.stderr)
        if payload:
            print(payload, file=sys.stderr)
        raise


def wait_for_polaris():
    print(f"Waiting for Polaris API and OAuth to become ready at {POLARIS_HOST}...")
    for attempt in range(1, 61):
        try:
            get_token()
            print("Polaris API and OAuth are ready.")
            return
        except Exception:
            if attempt == 60:
                raise RuntimeError("Polaris API and OAuth not ready after 60 attempts")
            time.sleep(5)


def get_token():
    print("Acquiring OAuth2 token...")
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    )
    _, data = request_json(
        f"{POLARIS_HOST}/api/catalog/v1/oauth/tokens",
        method="POST",
        body=body,
        content_type="application/x-www-form-urlencoded",
    )
    return data["access_token"]


def ensure_catalog(token):
    """Create the catalog if it doesn't exist, otherwise update properties.

    The Polaris Management API CREATE correctly persists storageConfigInfo
    fields (endpoint, pathStyleAccess, stsUnavailable, region), but the
    PUT/update API silently drops them. So storageConfigInfo is only set
    on create; updates only touch catalog properties.
    """
    headers = {"Authorization": f"Bearer {token}"}

    catalog_properties = {
        "default-base-location": BASE_LOCATION,
        "table-default.s3.endpoint": S3_ENDPOINT,
        "table-default.s3.access-key-id": S3_ACCESS_KEY,
        "table-default.s3.secret-access-key": S3_SECRET_KEY,
        "table-default.s3.path-style-access": "true",
        "table-default.s3.region": S3_REGION,
        # Garage rejects the eTag checksums that Iceberg's S3FileIO sends
        "table-default.s3.checksum-enabled": "false",
    }

    storage_config = {
        "storageType": "S3",
        "allowedLocations": [BASE_LOCATION],
        # Required by the Polaris API schema but unused when stsUnavailable=true
        "roleArn": "arn:aws:iam::000000000000:role/polaris",
        "endpoint": S3_ENDPOINT,
        "pathStyleAccess": True,
        "region": S3_REGION,
        # Garage has no STS endpoint - Polaris hands out raw S3 keys
        "stsUnavailable": True,
    }

    status, existing = request_json(
        f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}",
        headers=headers,
        allowed_statuses=(200, 404),
    )

    if status == 200:
        entity_version = existing["entityVersion"]
        print(
            f"Catalog '{CATALOG_NAME}' exists (entityVersion={entity_version}), "
            "updating properties..."
        )
        request_json(
            f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}",
            method="PUT",
            headers=headers,
            body={
                "currentEntityVersion": entity_version,
                "properties": catalog_properties,
            },
        )
        print(f"  Catalog '{CATALOG_NAME}' updated.")
    else:
        print(f"Creating catalog '{CATALOG_NAME}'...")
        request_json(
            f"{POLARIS_HOST}/api/management/v1/catalogs",
            method="POST",
            headers=headers,
            body={
                "catalog": {
                    "name": CATALOG_NAME,
                    "type": "INTERNAL",
                    "properties": catalog_properties,
                    "storageConfigInfo": storage_config,
                }
            },
            allowed_statuses=(200, 201),
        )
        print(f"  Catalog '{CATALOG_NAME}' created.")


def ensure_catalog_roles(token):
    """Create catalog roles and grant them CATALOG_MANAGE_CONTENT."""
    headers = {"Authorization": f"Bearer {token}"}
    print("Setting up catalog roles...")

    for role_name in CATALOG_ROLES:
        status, _ = request_json(
            f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}"
            f"/catalog-roles/{role_name}",
            headers=headers,
            allowed_statuses=(200, 404),
        )
        if status == 200:
            print(f"  Catalog role '{role_name}' already exists.")
        else:
            request_json(
                f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}"
                "/catalog-roles",
                method="POST",
                headers=headers,
                body={"catalogRole": {"name": role_name}},
            )
            print(f"  Catalog role '{role_name}' created.")

        _, grants = request_json(
            f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}"
            f"/catalog-roles/{role_name}/grants",
            headers=headers,
        )
        has_manage_content = any(
            grant.get("type") == "catalog"
            and grant.get("privilege") == "CATALOG_MANAGE_CONTENT"
            for grant in grants.get("grants", [])
        )
        if has_manage_content:
            print(f"  Catalog role '{role_name}' already has CATALOG_MANAGE_CONTENT.")
        else:
            request_json(
                f"{POLARIS_HOST}/api/management/v1/catalogs/{CATALOG_NAME}"
                f"/catalog-roles/{role_name}/grants",
                method="PUT",
                headers=headers,
                body={
                    "grant": {
                        "type": "catalog",
                        "privilege": "CATALOG_MANAGE_CONTENT",
                    }
                },
                allowed_statuses=(200, 201),
            )
            print(f"  Granted CATALOG_MANAGE_CONTENT to '{role_name}'.")

        _, assigned_roles = request_json(
            f"{POLARIS_HOST}/api/management/v1/principal-roles/service_admin"
            f"/catalog-roles/{CATALOG_NAME}",
            headers=headers,
        )
        is_assigned = any(
            assigned_role.get("name") == role_name
            for assigned_role in assigned_roles.get("roles", [])
        )
        if is_assigned:
            print(f"  Catalog role '{role_name}' already assigned to service_admin.")
        else:
            request_json(
                f"{POLARIS_HOST}/api/management/v1/principal-roles/service_admin"
                f"/catalog-roles/{CATALOG_NAME}",
                method="PUT",
                headers=headers,
                body={"catalogRole": {"name": role_name}},
                allowed_statuses=(200, 201),
            )
            print(f"  Assigned catalog role '{role_name}' to service_admin.")


def ensure_namespaces(token):
    """Create required Iceberg namespaces in the catalog."""
    headers = {"Authorization": f"Bearer {token}"}
    namespaces = ["default"]
    print("Setting up Iceberg namespaces...")

    for ns in namespaces:
        status, _ = request_json(
            f"{POLARIS_HOST}/api/catalog/v1/{CATALOG_NAME}/namespaces/{ns}",
            headers=headers,
            allowed_statuses=(200, 404),
        )
        if status == 200:
            print(f"  Namespace '{ns}' already exists.")
        else:
            request_json(
                f"{POLARIS_HOST}/api/catalog/v1/{CATALOG_NAME}/namespaces",
                method="POST",
                headers=headers,
                body={"namespace": [ns]},
                allowed_statuses=(200, 201),
            )
            print(f"  Namespace '{ns}' created.")


if __name__ == "__main__":
    wait_for_polaris()
    token = get_token()
    ensure_catalog(token)
    ensure_catalog_roles(token)
    ensure_namespaces(token)
    print("\n=== Polaris catalog initialization complete ===")
