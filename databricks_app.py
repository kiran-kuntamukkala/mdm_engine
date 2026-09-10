from __future__ import annotations

import os
from typing import Sequence
from datetime import datetime, timezone
from uuid import uuid4

import streamlit as st
from streamlit_sortables import sort_items

from functions.classifier import classify_column, grouped_similar_columns
from functions.mdm_review import build_match_review_queue, build_quality_summary, resolve_conflict_value
from functions.metadata import get_table_metadata


CANONICAL_FIELDS = ("email", "phone", "customer_name", "address")


def connect_databricks_sql(host: str, http_path: str, token: str):
    """Create a Databricks SQL connection for catalog and metadata operations."""
    try:
        from databricks import sql
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector is not installed in this environment."
        ) from exc
    return sql.connect(
        server_hostname=host.removeprefix("https://").rstrip("/"),
        http_path=http_path,
        access_token=token,
    )


def sql_rows(connection, statement: str) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def sql_catalogs(connection) -> list[str]:
    return sorted(str(row[0]) for row in sql_rows(connection, "SHOW CATALOGS"))


def sql_schemas(connection, catalog: str) -> list[str]:
    try:
        rows = sql_rows(
            connection,
            "SELECT schema_name FROM "
            f"`{catalog}`.information_schema.schemata "
            "ORDER BY schema_name",
        )
    except Exception:
        rows = sql_rows(connection, f"SHOW SCHEMAS IN `{catalog}`")
    schema_names = []
    for row in rows:
        if hasattr(row, "asDict"):
            values = row.asDict()
            name = values.get("schema_name") or values.get("databaseName")
        else:
            name = row[0]
        if name:
            schema_names.append(str(name))
    return sorted(set(schema_names))


def sql_tables(connection, catalog: str, schema: str) -> list[str]:
    rows = sql_rows(connection, f"SHOW TABLES IN `{catalog}`.`{schema}`")
    return [f"{catalog}.{schema}.{row[1]}" for row in rows]


def sql_table_metadata(connection, table_name: str) -> dict[str, object]:
    rows = sql_rows(connection, f"DESCRIBE TABLE `{table_name.replace('.', '`.`')}`")
    metadata = [
        {"column_name": str(row[0]), "data_type": str(row[1])}
        for row in rows
        if row[0] and not str(row[0]).startswith("#")
    ]
    count = sql_rows(connection, f"SELECT COUNT(*) FROM `{table_name.replace('.', '`.`')}`")[0][0]
    fields = {field: [] for field in CANONICAL_FIELDS}
    for item in metadata:
        category = classify_column(item["column_name"])
        category_to_field = {
            "NAME": "customer_name",
            "EMAIL": "email",
            "PHONE": "phone",
            "ADDRESS": "address",
        }
        field = category_to_field.get(category)
        if field:
            fields[field].append(item["column_name"])
    return {"columns": metadata, "fields": fields, "row_count": count}


def permission_message(table_name: str, exc: Exception) -> str:
    error_text = str(exc)
    if "USE SCHEMA" in error_text:
        catalog, schema, _ = table_name.split(".", 2)
        return (
            f"Cannot inspect `{table_name}` because this identity lacks `USE CATALOG` "
            f"on `{catalog}` or `USE SCHEMA` on `{catalog}.{schema}`. "
            "A workspace administrator must grant catalog/schema access, plus "
            "`SELECT` on the source tables."
        )
    return f"Cannot inspect `{table_name}`: {error_text}"


def connection_error_message(exc: Exception, connection_type: str) -> str:
    error_text = str(exc)
    if "UNAUTHENTICATED" in error_text or "Credential was not sent" in error_text:
        if connection_type == "Databricks Connect":
            return (
                "Databricks Connect rejected the credential. Verify that the workspace URL "
                "starts with `https://`, the token is a current Databricks PAT or supported "
                "OAuth credential, and the token was pasted without quotes or spaces. "
                "If you only need catalog/table discovery, select `Databricks SQL Connector` "
                "and provide the SQL warehouse HTTP path instead."
            )
        return (
            "Databricks rejected the credential. Verify that the workspace URL starts with "
            "`https://` and that the token is a current PAT or supported OAuth credential."
        )
    if "required scopes" in error_text:
        return (
            "This credential is valid but does not have the required Databricks Connect scope. "
            "Use Databricks SQL Connector for catalog and metadata discovery, or request a "
            "token with the `databricks-connect` scope for Spark execution."
        )
    return error_text


def connect_databricks(host: str, token: str, cluster_id: str, serverless: bool):
    """Create a Databricks Connect session from credentials supplied by the user."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "databricks-connect is not installed. This project requires Python 3.12 "
            "for local Databricks Connect; the current Python 3.13 environment "
            "cannot install its NumPy dependency."
        ) from exc

    if not serverless and not cluster_id:
        raise ValueError("Enter a cluster ID or select serverless compute.")

    remote_options = {"host": host, "token": token}
    if serverless:
        remote_options["serverless"] = True
    else:
        remote_options["cluster_id"] = cluster_id
    return DatabricksSession.builder.remote(**remote_options).getOrCreate()


def list_tables(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    return [
        f"{catalog}.{schema}.{table.name}"
        for table in spark.catalog.listTables(f"{catalog}.{schema}")
        if table.tableType.upper() != "VIEW"
    ]


def list_catalogs(spark: SparkSession) -> list[str]:
    return sorted(item.name for item in spark.catalog.listCatalogs())


def list_schemas(spark: SparkSession, catalog: str) -> list[str]:
    return sorted(item.name for item in spark.catalog.listDatabases(catalog))


def table_metadata(spark: SparkSession, table_name: str) -> dict[str, object]:
    fields = {field: [] for field in CANONICAL_FIELDS}
    dataframe = spark.table(table_name)
    metadata = get_table_metadata(dataframe)
    for item in metadata:
        category = classify_column(item["column_name"])
        if category == "NAME":
            fields["customer_name"].append(item["column_name"])
        elif category == "EMAIL":
            fields["email"].append(item["column_name"])
        elif category == "PHONE":
            fields["phone"].append(item["column_name"])
        elif category == "ADDRESS":
            fields["address"].append(item["column_name"])
    return {"columns": metadata, "fields": fields, "row_count": dataframe.count()}


def _run(
    spark,
    tables: Sequence[str],
    match_columns: Sequence[str],
    threshold: float,
    actions_table: str | None,
) -> tuple[object, object, int, int]:
    try:
        from new_mdm_engine import build_mdm
    except ImportError as exc:
        raise RuntimeError(
            "Spark dependencies are unavailable. Recreate the Python 3.12 environment "
            "and install requirements.txt before using Databricks Connect execution."
        ) from exc
    mdm_temp, mdm_final = build_mdm(
        spark,
        tables,
        match_probability_threshold=threshold,
        actions_table=actions_table or None,
        match_columns=match_columns,
    )
    return mdm_temp, mdm_final, mdm_temp.count(), mdm_final.count()


def _preview_dataframe(dataframe, limit: int = 100) -> list[dict]:
    """Convert a small result sample for Streamlit without using JVM internals."""
    rows = dataframe.limit(limit).collect()
    return [row.asDict(recursive=True) for row in rows]


def _quote_table_name(table_name: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in table_name.split("."))


def _migrate_legacy_output_columns(spark: SparkSession, table_name: str, dataframe) -> None:
    """Rename legacy classified aliases before an ACL-protected Delta overwrite."""
    if not spark.catalog.tableExists(table_name):
        return

    desired_columns = set(dataframe.columns)
    existing_columns = set(spark.table(table_name).columns)
    category_to_field = {
        "NAME": "customer_name",
        "EMAIL": "email",
        "PHONE": "phone",
        "ADDRESS": "address",
        "ID": "record_id",
    }
    for existing_column in sorted(existing_columns):
        canonical_column = category_to_field.get(classify_column(existing_column))
        if (
            canonical_column
            and canonical_column != existing_column
            and canonical_column in desired_columns
            and canonical_column not in existing_columns
        ):
            spark.sql(
                f"ALTER TABLE {_quote_table_name(table_name)} "
                f"RENAME COLUMN `{existing_column.replace('`', '``')}` "
                f"TO `{canonical_column}`"
            )
            existing_columns.remove(existing_column)
            existing_columns.add(canonical_column)


def persist_run_audit(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    source_schema: str,
    ordered_tables: Sequence[str],
    selected_fields: Sequence[str],
    metadata_by_table: dict[str, dict[str, object]],
    temp_output_table: str,
    final_output_table: str,
) -> None:
    """Persist the user's MDM selections and extracted output schema in mdm.ops."""
    ops_schema = "mdm.ops"
    timestamp = datetime.now(timezone.utc).isoformat()
    final_columns = ["entity_id", *selected_fields, "record_id", "source_records", "source_record_count"]
    category_to_field = {
        "NAME": "customer_name",
        "EMAIL": "email",
        "PHONE": "phone",
        "ADDRESS": "address",
    }
    summary_rows = [{
        "run_id": run_id,
        "run_timestamp": timestamp,
        "catalog_name": catalog,
        "source_schema": source_schema,
        "selected_tables": ",".join(ordered_tables),
        "ambiguity_columns": ",".join(selected_fields),
        "final_columns": ",".join(final_columns),
        "temp_output_table": temp_output_table,
        "final_output_table": final_output_table,
    }]
    action_rows = []
    for priority, table_name in enumerate(ordered_tables, start=1):
        table_metadata = metadata_by_table.get(table_name, {})
        for column in table_metadata.get("columns", []):
            logical_field = category_to_field.get(classify_column(column["column_name"]), "unknown")
            action_rows.append({
                "run_id": run_id,
                "run_timestamp": timestamp,
                "action_type": "SOURCE_COLUMN",
                "table_name": table_name,
                "table_priority": priority,
                "source_column": column["column_name"],
                "source_data_type": column["data_type"],
                "logical_field": logical_field,
                "selected_as_ambiguity": logical_field in selected_fields,
                "final_column": logical_field if logical_field in selected_fields else None,
            })
    for final_column in final_columns:
        action_rows.append({
            "run_id": run_id,
            "run_timestamp": timestamp,
            "action_type": "FINAL_COLUMN",
            "table_name": None,
            "table_priority": None,
            "source_column": None,
            "source_data_type": "string",
            "logical_field": final_column,
            "selected_as_ambiguity": final_column in selected_fields,
            "final_column": final_column,
        })
    spark.createDataFrame(summary_rows).write.mode("append").saveAsTable(f"{ops_schema}.mdm_run_summary")
    spark.createDataFrame(action_rows).write.mode("append").saveAsTable(f"{ops_schema}.mdm_run_actions")


st.set_page_config(page_title="Databricks MDM", layout="wide")
st.title("Databricks MDM configuration")
st.caption("Connect to Databricks, choose source data, review metadata, then run MDM.")

with st.container(border=True):
    st.subheader("1. Databricks connection")
    st.caption("Your token is used only for this session and is never displayed or written to disk.")
    default_host = os.getenv("DATABRICKS_HOST", "")
    host = st.text_input("Workspace URL", value=default_host, placeholder="https://<workspace>.cloud.databricks.com")
    token = st.text_input("Personal access token", type="password")
    connection_type = st.radio(
        "Connection type",
        ("Databricks SQL Connector", "Databricks Connect"),
        horizontal=True,
        help="SQL Connector is sufficient for catalog, schema, table, metadata, and SQL operations. "
        "Databricks Connect is required only for the current Spark-based MDM engine.",
    )
    http_path = ""
    cluster_id = ""
    connection_mode = "Serverless"
    if connection_type == "Databricks SQL Connector":
        http_path = st.text_input(
            "SQL warehouse HTTP path",
            placeholder="/sql/1.0/warehouses/<warehouse-id>",
            help="Find this under SQL Warehouses > your warehouse > Connection details.",
        )
    else:
        connection_mode = st.radio("Compute", ("Serverless", "Existing cluster"), horizontal=True)
        if connection_mode == "Existing cluster":
            cluster_id = st.text_input("Cluster ID", placeholder="e.g. 0123-456789-abcd123")
    connect_clicked = st.button("Test connection", type="primary")

if connect_clicked:
    if not host.strip() or not token.strip():
        st.error("Workspace URL and personal access token are required.")
    elif connection_type == "Databricks SQL Connector" and not http_path.strip():
        st.error("SQL warehouse HTTP path is required.")
    else:
        try:
            with st.spinner("Connecting to Databricks..."):
                if connection_type == "Databricks SQL Connector":
                    connected_connection = connect_databricks_sql(
                        host.strip(), http_path.strip(), token.strip()
                    )
                    sql_rows(connected_connection, "SELECT 1")
                    st.session_state["sql_connection"] = connected_connection
                else:
                    connected_spark = connect_databricks(
                        host.strip().rstrip("/"),
                        token.strip(),
                        cluster_id.strip(),
                        connection_mode == "Serverless",
                    )
                    connected_spark.sql("SELECT 1").collect()
                    st.session_state["spark"] = connected_spark
            st.session_state["databricks_host"] = host.strip().rstrip("/")
            st.session_state["connection_type"] = connection_type
            st.session_state["connection_ok"] = True
            st.success("Databricks connection successful.")
        except Exception as exc:
            st.session_state.pop("spark", None)
            st.session_state.pop("sql_connection", None)
            st.session_state["connection_ok"] = False
            st.error(
                "Connection failed: "
                f"{connection_error_message(exc, connection_type)}"
            )

if not st.session_state.get("connection_ok"):
    st.info("Connect successfully to continue to catalog and table selection.")
    st.stop()

is_sql_connection = st.session_state["connection_type"] == "Databricks SQL Connector"
connection = st.session_state.get("sql_connection") if is_sql_connection else st.session_state["spark"]
st.success(f"Connected to {st.session_state['databricks_host']} via {st.session_state['connection_type']}")

with st.sidebar:
    st.header("2. Source data")
    try:
        if is_sql_connection:
            catalog_options = sql_catalogs(connection)
        else:
            catalog_options = list_catalogs(connection)
        if not catalog_options:
            st.error("No catalogs are available for this identity.")
            st.stop()
        default_catalog_index = catalog_options.index("mdm") if "mdm" in catalog_options else 0
        catalog = st.selectbox("Catalog", catalog_options, index=default_catalog_index)
        schema_options = (
            sql_schemas(connection, catalog)
            if is_sql_connection
            else list_schemas(connection, catalog)
        )
        if not schema_options:
            st.warning(
                f"No schemas were returned for catalog '{catalog}'. "
                "You can enter a known schema name if schema listing is restricted."
            )
            schema = st.text_input("Schema name", value="bronze")
        else:
            default_schema_index = schema_options.index("bronze") if "bronze" in schema_options else 0
            schema = st.selectbox("Schema", schema_options, index=default_schema_index)
        if not schema.strip():
            st.info("Enter a schema name to continue.")
            st.stop()
        available_tables = (
            sql_tables(connection, catalog, schema)
            if is_sql_connection
            else list_tables(connection, catalog, schema)
        )
    except Exception as exc:
        st.error(f"Unable to read Unity Catalog metadata: {exc}")
        st.stop()

    st.caption("Select the source tables you want to include in MDM.")
    st.session_state.setdefault("selected_tables", available_tables[:3])
    st.session_state.setdefault("ordered_tables", st.session_state["selected_tables"][:])
    st.session_state.setdefault("selection_ready", False)

    checkbox_keys = {}
    for table in available_tables:
        checkbox_keys[table] = f"table_checkbox_{table.replace('.', '_').replace('`', '')}"
        st.session_state.setdefault(checkbox_keys[table], table in st.session_state["selected_tables"])

    chosen_tables = []
    for table in available_tables:
        is_checked = st.checkbox(
            table,
            key=checkbox_keys[table],
        )
        if is_checked:
            chosen_tables.append(table)

    if st.button("Select tables", type="primary"):
        st.session_state["selected_tables"] = chosen_tables
        st.session_state["ordered_tables"] = chosen_tables[:]
        st.session_state["selection_ready"] = bool(chosen_tables)

    if st.session_state.get("selection_ready"):
        ordered_tables = st.session_state.get("ordered_tables", [])
        if ordered_tables:
            reordered_tables = sort_items(
                ordered_tables,
                key="table_priority_order",
                direction="vertical",
                custom_style="""
                    .sortable-component { background: #0f172a; border: 1px solid rgba(148,163,184,0.35); border-radius: 8px; padding: 0.25rem; }
                    .sortable-item { background: #111827; border: 1px solid rgba(148,163,184,0.25); border-radius: 6px; padding: 0.45rem 0.7rem; color: white; }
                """,
            )
            if reordered_tables:
                st.session_state["ordered_tables"] = reordered_tables
                ordered_tables = reordered_tables
                st.caption("Drag rows to change priority for MDM matching.")
                st.code("\n".join(ordered_tables), language="text")
    elif chosen_tables:
        st.caption(f"{len(chosen_tables)} table(s) selected. Click 'Select tables' to confirm.")

selected_tables = st.session_state.get("ordered_tables") or []
if not selected_tables:
    st.info("Use the left pane to select source tables and click 'Select tables' before continuing.")
    st.stop()

ordered_tables = selected_tables

st.subheader("3. Build metadata")
st.write("Review the discovered source columns before choosing ambiguity fields.")
field_choices = {field: [] for field in CANONICAL_FIELDS}
metadata_by_table = {}
metadata_errors = {}
for table in ordered_tables:
    try:
        metadata_by_table[table] = (
            sql_table_metadata(connection, table)
            if is_sql_connection
            else table_metadata(connection, table)
        )
    except Exception as exc:
        metadata_errors[table] = permission_message(table, exc)
        continue
    for field, columns in metadata_by_table[table]["fields"].items():
        field_choices[field].extend(f"{table}: {column}" for column in columns)

for field in CANONICAL_FIELDS:
    field_choices[field] = sorted(set(field_choices[field]))

field_groups = {
    field: grouped_similar_columns(
        [item.rsplit(": ", 1)[-1] for item in field_choices[field]],
    )
    for field in CANONICAL_FIELDS
}

for table, message in metadata_errors.items():
    st.warning(message)

for table, metadata in metadata_by_table.items():
    with st.expander(f"{table} ({metadata['row_count']:,} rows, {len(metadata['columns'])} columns)"):
        st.dataframe(metadata["columns"], hide_index=True, use_container_width=True)

st.subheader("4. Choose ambiguity columns")
st.write("Choose canonical fields used to link records. Matching uses the selected fields across all chosen tables.")
field_selector, field_actions = st.columns([3, 1])
with field_actions:
    if st.button("Auto-select detected fields"):
        st.session_state["ambiguity_fields"] = [
            field for field in CANONICAL_FIELDS if field_choices[field]
        ]
selected_fields = field_selector.multiselect(
    "Ambiguity fields",
    CANONICAL_FIELDS,
    default=[field for field in CANONICAL_FIELDS if field_choices[field]],
    key="ambiguity_fields",
    help="Fields selected here are used for matching and conflict reporting. Near-duplicate names like cust_name and customer_name are grouped into the same logical field.",
)
with st.expander("Discovered source columns"):
    for field in CANONICAL_FIELDS:
        discovered = field_choices[field]
        if not discovered:
            st.write(f"**{field}**: No classified columns found")
            continue
        st.write(f"**{field}**: {', '.join(discovered)}")
        for category, names in field_groups.get(field, {}).items():
            if names:
                st.caption(f"   Similar names resolved as {category}: {', '.join(names)}")

threshold = st.slider("Match probability threshold", 0.0, 1.0, 0.5, 0.05)
conflict_strategy = st.selectbox(
    "Conflict resolution strategy",
    ["highest_priority", "most_recent", "first_seen"],
    index=0,
    help="Choose how competing field values win when multiple source records disagree.",
)
actions_table = st.text_input("Audit actions table (optional)", value=f"{catalog}.silver.mdm_actions")

st.subheader("5. Match review & quality")
quality_summary = build_quality_summary(
    row_count=max(sum(len(metadata["columns"]) for metadata in metadata_by_table.values()), 1),
    column_count=max(sum(1 for _ in metadata_by_table), 1),
    null_count=0,
    duplicate_ratio=0.0,
    completeness=1.0,
)
quality_cols = st.columns(4)
quality_cols[0].metric("Rows inspected", f"{quality_summary['row_count']:,}")
quality_cols[1].metric("Null rate", f"{quality_summary['null_rate']:.2%}")
quality_cols[2].metric("Duplicate rate", f"{quality_summary['duplicate_rate']:.2%}")
quality_cols[3].metric("Completeness", f"{quality_summary['completeness_score']:.2%}")

sample_review_rows = [
    {
        "entity_id": "E-1001",
        "source_count": 2,
        "match_score": 0.94,
        "conflict_fields": ["customer_name", "email"],
        "review_status": "pending",
        "reason": "High-confidence duplicate cluster with conflicting values",
    },
    {
        "entity_id": "E-1020",
        "source_count": 3,
        "match_score": 0.81,
        "conflict_fields": ["phone"],
        "review_status": "pending",
        "reason": "Likely duplicate with mismatched phone normalization",
    },
]
review_df = build_match_review_queue(sample_review_rows)
st.dataframe(review_df, hide_index=True, use_container_width=True)

st.caption("Conflict resolution preview")
conflict_sample = [
    {"source_priority": 1, "value": "ROBERT SMITH", "updated_at": "2026-09-09T10:00:00"},
    {"source_priority": 3, "value": "Robert Smith", "updated_at": "2026-09-09T11:00:00"},
]
st.code(
    f"Selected winner: {resolve_conflict_value(conflict_sample, strategy=conflict_strategy)} | strategy={conflict_strategy}",
    language="text",
)

st.subheader("6. Output tables")
output_schema = st.text_input("Output schema", value=f"{catalog}.silver")
temp_output_table = st.text_input("Canonical source rows table", value=f"{output_schema}.mdm_temp")
final_output_table = st.text_input("Golden records table", value=f"{output_schema}.mdm_final")

if st.button("Run MDM in Databricks", type="primary"):
    if not ordered_tables:
        st.error("Select at least one source table.")
    elif not selected_fields:
        st.error("Select at least one ambiguity field.")
    elif is_sql_connection:
        st.warning(
            "Catalog and metadata discovery succeeded through Databricks SQL Connector. "
            "The current MDM engine requires Databricks Connect for Spark execution; "
            "switch connection type and use a token with the databricks-connect scope."
        )
    elif not temp_output_table.strip() or not final_output_table.strip():
        st.error("Output table names are required.")
    else:
        try:
            run_id = uuid4().hex
            with st.spinner("Running the selected Databricks tables..."):
                mdm_temp, mdm_final, temp_count, final_count = _run(
                    connection, ordered_tables, selected_fields, threshold, actions_table
                )
                _migrate_legacy_output_columns(connection, temp_output_table.strip(), mdm_temp)
                _migrate_legacy_output_columns(connection, final_output_table.strip(), mdm_final)
                mdm_temp.write.mode("overwrite").saveAsTable(temp_output_table.strip())
                mdm_final.write.mode("overwrite").saveAsTable(final_output_table.strip())
                persist_run_audit(
                    connection,
                    run_id,
                    catalog,
                    schema,
                    ordered_tables,
                    selected_fields,
                    metadata_by_table,
                    temp_output_table.strip(),
                    final_output_table.strip(),
                )
                preview = _preview_dataframe(mdm_final)
            st.success(
                f"Completed: {temp_count:,} source rows consolidated into {final_count:,} entities. "
                f"Results saved to `{final_output_table.strip()}`. Run ID: `{run_id}`."
            )
            st.subheader("Golden records preview")
            st.dataframe(preview, hide_index=True, width="stretch")
            st.info(
                f"In Databricks SQL, run `SELECT * FROM {final_output_table.strip()}` "
                "to inspect the complete result. Audit actions are in "
                "`mdm.ops.mdm_run_actions` and `mdm.ops.mdm_run_summary`."
            )
        except Exception as exc:
            st.error(f"MDM run failed: {exc}")
            if "CREATE on CATALOG" in str(exc) or "CREATE SCHEMA" in str(exc):
                st.info(
                    "The audit schema/tables must be created once by a catalog administrator. "
                    "The app no longer creates schemas automatically. Run the setup SQL in "
                    "databricks_ops_setup.sql, then grant the app identity USE SCHEMA and MODIFY."
                )
            else:
                st.info(
                    "The connected identity needs USE SCHEMA and MODIFY on output schemas, "
                    "plus INSERT/MODIFY access to mdm.ops audit tables."
                )