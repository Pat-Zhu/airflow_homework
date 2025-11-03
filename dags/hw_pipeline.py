from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os, requests, pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR = "/opt/airflow/include"
RAW_DIR = os.path.join(BASE_DIR, "data/raw")
PROC_DIR = os.path.join(BASE_DIR, "data/processed")
OUT_DIR = os.path.join(BASE_DIR, "outputs")

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROC_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

def ingest_population(ti):
    ensure_dirs()
    url = "https://raw.githubusercontent.com/jakevdp/data-USstates/master/state-population.csv"
    p = os.path.join(RAW_DIR, "state_population.csv")
    r = requests.get(url, timeout=60); r.raise_for_status()
    open(p, "wb").write(r.content)
    ti.xcom_push(key="pop_raw", value=p)

def ingest_areas(ti):
    ensure_dirs()
    url = "https://raw.githubusercontent.com/jakevdp/data-USstates/master/state-areas.csv"
    p = os.path.join(RAW_DIR, "state_areas.csv")
    r = requests.get(url, timeout=60); r.raise_for_status()
    open(p, "wb").write(r.content)
    ti.xcom_push(key="area_raw", value=p)

def transform_population(ti):
    ensure_dirs()
    p = ti.xcom_pull(key="pop_raw", task_ids="ingest.ingest_population")
    df = pd.read_csv(p)
    df = df[(df["ages"]=="total") & (df["year"]==2012)]
    m = {"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia"}
    df["state"] = df["state/region"].map(m)
    df = df.dropna(subset=["state"])
    df = df.groupby("state", as_index=False)["population"].sum()
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype("int64")
    out = os.path.join(PROC_DIR, "pop_2012.csv")
    df.to_csv(out, index=False)
    ti.xcom_push(key="pop_proc", value=out)

def transform_areas(ti):
    ensure_dirs()
    p = ti.xcom_pull(key="area_raw", task_ids="ingest.ingest_areas")
    df = pd.read_csv(p)
    cols = list(df.columns)
    if "area (sq. mi)" in cols: area_col = "area (sq. mi)"
    elif "area (sq. mi.)" in cols: area_col = "area (sq. mi.)"
    else: area_col = cols[-1]
    df = df[["state", area_col]].rename(columns={area_col: "area"})
    df["area"] = pd.to_numeric(df["area"], errors="coerce")
    out = os.path.join(PROC_DIR, "areas.csv")
    df.to_csv(out, index=False)
    ti.xcom_push(key="area_proc", value=out)

def merge_datasets(ti):
    ensure_dirs()
    p1 = ti.xcom_pull(key="pop_proc", task_ids="transform.transform_population")
    p2 = ti.xcom_pull(key="area_proc", task_ids="transform.transform_areas")
    pop = pd.read_csv(p1); areas = pd.read_csv(p2)
    pop["state"] = pop["state"].str.strip(); areas["state"] = areas["state"].str.strip()
    df = pop.merge(areas, on="state", how="inner")
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df["area"] = pd.to_numeric(df["area"], errors="coerce")
    df = df.dropna(subset=["population","area"])
    df = df[df["area"]>0]
    df["density"] = df["population"] / df["area"]
    out = os.path.join(OUT_DIR, "state_density.csv")
    df.to_csv(out, index=False)
    ti.xcom_push(key="final_csv", value=out)

def load_to_postgres(ti):
    csv_path = ti.xcom_pull(key="final_csv", task_ids="merge")
    df = pd.read_csv(csv_path)
    hook = PostgresHook(postgres_conn_id="warehouse_postgres")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql("state_density", engine, schema="public", if_exists="replace", index=False, method="multi", chunksize=1000)

def analyze(ti):
    hook = PostgresHook(postgres_conn_id="warehouse_postgres")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql("select state, density from public.state_density order by density desc limit 10", engine)
    fig, ax = plt.subplots(figsize=(8,5))
    ax.barh(df["state"][::-1], df["density"][::-1])
    ax.set_xlabel("density"); ax.set_ylabel("state"); ax.set_title("Top 10 Density")
    p = os.path.join(OUT_DIR, "top10_density.png")
    plt.tight_layout(); fig.savefig(p)
    ti.xcom_push(key="plot_path", value=p)

def cleanup():
    for d in [RAW_DIR, PROC_DIR]:
        if os.path.isdir(d):
            for f in os.listdir(d):
                fp = os.path.join(d, f)
                try:
                    if os.path.isfile(fp): os.remove(fp)
                except: pass

with DAG(
    dag_id="hw_pipeline",
    start_date=datetime(2024,1,1),
    schedule="0 9 * * *",
    catchup=False,
    default_args={"owner":"patrick","retries":1},
    tags=["homework","parallel"]
):
    start = EmptyOperator(task_id="start")
    with TaskGroup(group_id="ingest") as ingest:
        t1 = PythonOperator(task_id="ingest_population", python_callable=ingest_population)
        t2 = PythonOperator(task_id="ingest_areas", python_callable=ingest_areas)
    with TaskGroup(group_id="transform") as transform:
        t3 = PythonOperator(task_id="transform_population", python_callable=transform_population)
        t4 = PythonOperator(task_id="transform_areas", python_callable=transform_areas)
    merge = PythonOperator(task_id="merge", python_callable=merge_datasets)
    load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    analyze_task = PythonOperator(task_id="analyze", python_callable=analyze)
    clean = PythonOperator(task_id="cleanup", python_callable=cleanup)
    end = EmptyOperator(task_id="end")
    start >> ingest >> transform
    [t3, t4] >> merge >> load >> analyze_task >> clean >> end
