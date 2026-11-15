#!/usr/bin/env python3
"""Rampe de debit sur le pipeline olist-flink-streaming.

Principe : pour chaque debit cible, on injecte une charge soutenue pendant
DURATION secondes, on echantillonne les metriques Flink exposees a Prometheus,
et on mesure le debit reellement produit aux offsets du broker Kafka plutot
qu'en croyant le generateur sur parole.

La rampe s'arrete d'elle-meme quand le pipeline ne resorbe plus son retard
entre deux paliers : c'est le point de saturation, et c'est la mesure qui
compte. Le reste (occupation, contre-pression, checkpoints) sert a identifier
QUEL maillon sature.

Depend uniquement de la bibliotheque standard et du binaire docker.
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

FLINK = "http://localhost:8081"
PROM = "http://localhost:9090"
TOPIC = "orders"
KAFKA_CONTAINER = "docker-kafka-1"
NETWORK = "docker_default"
JAR_NAME = "olist-flink-streaming-1.0-SNAPSHOT.jar"

def _env_list(name, default):
    raw = os.environ.get(name)
    return [int(x) for x in raw.split(",")] if raw else default


RATES = _env_list("BENCH_RATES", [100, 250, 500, 1000, 2000, 4000])
DURATION = int(os.environ.get("BENCH_DURATION", "90"))  # secondes de charge par palier
EXPECTED_JOBS = int(os.environ.get("BENCH_EXPECTED_JOBS", "3"))
RESULTS_NAME = os.environ.get("BENCH_RESULTS_NAME", "results.json")
LABEL = os.environ.get("BENCH_LABEL", "")
SAMPLE_EVERY = 10      # secondes entre deux echantillons
WARMUP_FRACTION = 1 / 3  # part initiale ecartee du regime etabli
DRAIN_TIMEOUT = 120    # secondes accordees pour resorber le retard
DRAIN_THRESHOLD = 2000  # records de retard consideres comme resorbes


# --------------------------------------------------------------------------- #
# utilitaires

def run(cmd, timeout=180):
    """Execute une commande et renvoie stdout, ou None en cas d'echec."""
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None
    return res.stdout if res.returncode == 0 else None


def get_json(url, timeout=15):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


def promql(query):
    """Renvoie la liste (labels, valeur) pour une requete instantanee."""
    url = PROM + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    data = get_json(url)
    if not data or data.get("status") != "success":
        return []
    out = []
    for serie in data["data"]["result"]:
        try:
            out.append((serie["metric"], float(serie["value"][1])))
        except (KeyError, ValueError):
            continue
    return out


def topic_end_offset():
    """Somme des offsets de fin du topic, mesuree au broker."""
    out = run([
        "docker", "exec", KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "localhost:9092",
        "--topic", TOPIC, "--time", "latest",
    ])
    if out is None:
        return None
    total = 0
    for line in out.strip().splitlines():
        # format : topic:partition:offset
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].lstrip("-").isdigit():
            total += int(parts[2])
    return total


# --------------------------------------------------------------------------- #
# mesures

def source_tasks():
    """Series des taches qui lisent reellement Kafka, par job."""
    return [(m, v) for m, v in promql("flink_taskmanager_job_task_numRecordsInPerSecond")
            if m.get("task_name", "").startswith("Source:_orders_source")]


def sample():
    """Un echantillon instantane de l'etat du pipeline."""
    s = {"ts": time.time(), "jobs": {}}

    for metric, key in (
        ("flink_taskmanager_job_task_numRecordsInPerSecond", "records_in_per_s"),
        ("flink_taskmanager_job_task_busyTimeMsPerSecond", "busy_ms_per_s"),
        ("flink_taskmanager_job_task_backPressuredTimeMsPerSecond", "backpressure_ms_per_s"),
    ):
        for m, v in promql(metric):
            job = m.get("job_name")
            task = m.get("task_name", "")
            if not job:
                continue
            entry = s["jobs"].setdefault(job, {})
            # On retient le maximum sur les taches du job : c'est le maillon
            # le plus charge qui determine le comportement du job.
            entry[key] = max(entry.get(key, 0.0), v)
            if key == "records_in_per_s" and task.startswith("Source:_orders_source"):
                entry["source_records_in_per_s"] = v

    for m, v in promql("flink_taskmanager_job_task_operator_pendingRecords"):
        job = m.get("job_name")
        if job:
            s["jobs"].setdefault(job, {})["pending_records"] = v

    for metric, key in (
        ("flink_jobmanager_job_lastCheckpointDuration", "checkpoint_ms"),
        ("flink_jobmanager_job_lastCheckpointFullSize", "checkpoint_bytes"),
        ("flink_jobmanager_job_numberOfCompletedCheckpoints", "checkpoints_ok"),
        ("flink_jobmanager_job_numberOfFailedCheckpoints", "checkpoints_ko"),
    ):
        for m, v in promql(metric):
            job = m.get("job_name")
            if job:
                s["jobs"].setdefault(job, {})[key] = v

    return s


def total_pending():
    vals = [v for _, v in promql("flink_taskmanager_job_task_operator_pendingRecords")]
    return max(vals) if vals else 0.0


def container_cpu():
    """Occupation CPU par conteneur, en pourcentage d'un coeur."""
    out = run(["docker", "stats", "--no-stream", "--format", "{{.Name}};{{.CPUPerc}};{{.MemUsage}}"])
    if out is None:
        return {}
    cpu = {}
    for line in out.strip().splitlines():
        parts = line.split(";")
        if len(parts) == 3:
            try:
                cpu[parts[0]] = {"cpu_pct": float(parts[1].rstrip("%")), "mem": parts[2]}
            except ValueError:
                continue
    return cpu


# --------------------------------------------------------------------------- #
# deroulement

def jobs_running():
    data = get_json(FLINK + "/jobs")
    if not data:
        return 0
    return sum(1 for j in data["jobs"] if j["status"] == "RUNNING")


def start_simulator(project_dir, rate, duration):
    """Lance le simulateur en conteneur, detache, sur le reseau du compose."""
    return subprocess.Popen([
        "docker", "run", "--rm", "--name", "olist-bench-sim",
        "--network", NETWORK,
        "-v", f"{project_dir}/build/libs:/libs:ro",
        "-v", f"{project_dir}/data:/data:ro",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-e", "OLIST_DATA_PATH=/data/",
        "-e", f"EVENTS_PER_SECOND={rate}",
        "-e", f"SIMULATOR_DURATION_SECONDS={duration}",
        "eclipse-temurin:21-jre",
        "java", "-cp", f"/libs/{JAR_NAME}",
        "com.olist.streaming.simulator.OlistEventSimulator",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def drain(label):
    """Attend la resorption du retard. False si le pipeline n'y arrive pas."""
    print(f"    resorption du retard ({label})...", end="", flush=True)
    deadline = time.time() + DRAIN_TIMEOUT
    while time.time() < deadline:
        pending = total_pending()
        if pending <= DRAIN_THRESHOLD:
            print(f" ok, retard {pending:.0f}")
            return True
        time.sleep(5)
    pending = total_pending()
    print(f" NON resorbe, retard {pending:.0f}")
    return False


def run_rate(project_dir, rate):
    print(f"\n--- palier {rate} ev/s pendant {DURATION}s ---")

    running = jobs_running()
    if running < EXPECTED_JOBS:
        print(f"    ATTENTION : {running} job(s) RUNNING, {EXPECTED_JOBS} attendu(s)")

    off0 = topic_end_offset()
    t0 = time.time()

    proc = start_simulator(project_dir, rate, DURATION)

    samples = []
    # Le simulateur charge d'abord les CSV : on echantillonne jusqu'a sa sortie.
    while proc.poll() is None:
        time.sleep(SAMPLE_EVERY)
        samples.append(sample())
        last = samples[-1]["jobs"]
        src = max((j.get("source_records_in_per_s", 0.0) for j in last.values()), default=0.0)
        pend = max((j.get("pending_records", 0.0) for j in last.values()), default=0.0)
        print(f"    t+{int(time.time() - t0):3d}s  source {src:7.0f} rec/s  retard {pend:8.0f}")

    t1 = time.time()
    off1 = topic_end_offset()
    cpu = container_cpu()

    # Deux debits distincts, a ne pas confondre :
    #  - sending_per_s : messages / duree d'envoi. C'est le debit que le
    #    generateur a reellement tenu, et le chiffre a publier.
    #  - wallclock_per_s : messages / temps total du conteneur, chargement des
    #    CSV et demarrage compris. Systematiquement plus bas, sans interet ici.
    produced = (off1 - off0) if (off0 is not None and off1 is not None) else None
    sending_per_s = (produced / DURATION) if produced is not None else None
    wallclock_per_s = (produced / (t1 - t0)) if (produced is not None and t1 > t0) else None

    # Regime etabli : on ecarte la phase initiale.
    keep = samples[int(len(samples) * WARMUP_FRACTION):] or samples

    def agg(job, key):
        vals = [s["jobs"].get(job, {}).get(key) for s in keep]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    job_names = sorted({name for s in keep for name in s["jobs"]})
    per_job = {}
    for job in job_names:
        per_job[job] = {
            "source_records_in_per_s": agg(job, "source_records_in_per_s"),
            "busy_pct": (agg(job, "busy_ms_per_s") or 0) / 10.0,
            "backpressure_pct": (agg(job, "backpressure_ms_per_s") or 0) / 10.0,
            "pending_records_max": max(
                (s["jobs"].get(job, {}).get("pending_records", 0.0) for s in keep), default=0.0),
            "checkpoint_ms": agg(job, "checkpoint_ms"),
            "checkpoint_bytes": agg(job, "checkpoint_bytes"),
            "checkpoints_ok": max(
                (s["jobs"].get(job, {}).get("checkpoints_ok", 0.0) for s in keep), default=0.0),
            "checkpoints_ko": max(
                (s["jobs"].get(job, {}).get("checkpoints_ko", 0.0) for s in keep), default=0.0),
        }

    drained = drain(f"apres {rate} ev/s")

    return {
        "target_rate": rate,
        "sending_duration_s": DURATION,
        "wallclock_s": round(t1 - t0, 1),
        "sending_per_s": sending_per_s,
        "wallclock_per_s": wallclock_per_s,
        "messages_produced": produced,
        "samples": len(samples),
        "jobs": per_job,
        "cpu": cpu,
        "drained": drained,
    }


def main():
    project_dir = str(Path.home() / "olist-flink-streaming")
    out_dir = Path(project_dir) / "benchmark-results"
    out_dir.mkdir(exist_ok=True)

    ov = get_json(FLINK + "/overview") or {}
    env = {
        "flink_version": ov.get("flink-version"),
        "taskmanagers": ov.get("taskmanagers"),
        "slots_total": ov.get("slots-total"),
        "iceberg_enabled": False,
        "checkpoint_storage": "file:///opt/flink/data/checkpoints",
    }
    desc = run(["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092", "--describe", "--topic", TOPIC])
    if desc:
        # kafka-topics.sh separe l'etiquette de sa valeur par une tabulation :
        # "PartitionCount:" et "1" sont deux jetons distincts.
        match = re.search(r"PartitionCount:\s*(\d+)", desc)
        if match:
            env["orders_partitions"] = int(match.group(1))

    # Parallelisme effectif des jobs en cours, lu sur le cluster plutot que
    # suppose : c'est la variable comparee entre les deux configurations.
    jobs_meta = get_json(FLINK + "/jobs/overview") or {"jobs": []}
    env["jobs"] = {}
    for j in jobs_meta["jobs"]:
        if j["state"] != "RUNNING":
            continue
        detail = get_json(FLINK + "/jobs/" + j["jid"])
        if detail:
            env["jobs"][j["name"]] = max(v["parallelism"] for v in detail["vertices"])

    print("### environnement")
    for k, v in env.items():
        print(f"  {k}: {v}")

    results = []
    for rate in RATES:
        res = run_rate(project_dir, rate)
        results.append(res)
        if res["sending_per_s"] is not None:
            print(f"    => {res['messages_produced']} msg au broker, "
                  f"soit {res['sending_per_s']:.0f} rec/s sur {DURATION}s d'envoi")
        else:
            print("    => debit produit non mesurable")
        if not res["drained"]:
            print(f"\n### saturation atteinte a {rate} ev/s : le retard ne se resorbe plus.")
            print("### arret de la rampe.")
            break

    payload = {
        "label": LABEL,
        "env": env,
        "results": results,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (out_dir / RESULTS_NAME).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\n### resultats ecrits dans {out_dir / RESULTS_NAME}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
