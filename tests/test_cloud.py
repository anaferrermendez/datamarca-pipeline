import os
from pathlib import Path

from src import cloud_entry
from src.cloud_utils import publish_reports
from src.config import create_spark_session, load_config


def test_cloud_entry_traduce_argumentos_a_variables(monkeypatch):
    for var in ("DATAMARCA_INPUT_DIR", "DATAMARCA_OUTPUT_DIR", "DATAMARCA_SPARK_MASTER",
                "DATAMARCA_REPORTS_URI", "DATAMARCA_SHUFFLE_PARTITIONS"):
        monkeypatch.delenv(var, raising=False)
    args = cloud_entry.parse_args(["--input-dir", "gs://b/raw", "--output-dir", "gs://b/out",
                                   "--reports-uri", "gs://b/rep", "--shuffle-partitions", "64"])
    cloud_entry.apply_environment(args)
    config = load_config()
    assert config.input_dir == "gs://b/raw"
    assert config.output_dir == "gs://b/out"
    assert config.spark_master == "auto"
    assert config.shuffle_partitions == 64


def test_master_auto_no_fija_master(monkeypatch):
    monkeypatch.setenv("DATAMARCA_SPARK_MASTER", "auto")
    # Con "auto" el builder no llama a .master(); aquí solo se comprueba la configuración
    assert load_config().spark_master == "auto"


def test_publish_reports_copia_a_destino(spark, tmp_path):
    local = tmp_path / "local"
    local.mkdir()
    (local / "metricas.json").write_text('{"ok": true}', encoding="utf-8")
    dest = tmp_path / "destino"
    used = publish_reports(spark, str(local), dest.as_uri())
    assert used == dest.as_uri()
    assert (dest / "metricas.json").read_text(encoding="utf-8") == '{"ok": true}'


def test_publish_reports_sin_destino_no_hace_nada(spark, tmp_path, monkeypatch):
    monkeypatch.delenv("DATAMARCA_REPORTS_URI", raising=False)
    assert publish_reports(spark, str(tmp_path)) is None
