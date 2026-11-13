from pathlib import Path

from solution.lead_ingestion import ingest_leads


def test_ingestion_merges_duplicates_and_protects_pii(tmp_path: Path):
    schema_root = tmp_path / "schemas"
    schema_root.mkdir()
    (schema_root / "workflow.json").write_text(
        '{"workflow_id":"workflow","fields":['
        '{"name":"customer_name","type":"string","required":true,"pii":true},'
        '{"name":"phone","type":"phone","required":true,"pii":true},'
        '{"name":"reference","type":"string","required":true,"reference":true},'
        '{"name":"language","type":"enum","required":false,"values":["en","hi"],"default":"en"}'
        ']}'
    )
    csv_dir = tmp_path / "data" / "drop_1" / "leads" / "workflow"
    csv_dir.mkdir(parents=True)
    (csv_dir / "batch_001.csv").write_text(
        "customer_name,phone,reference,language\n"
        "Alice,9876543210,R1,\n"
        "Bad,12345,R2,en\n",
        encoding="utf-8",
    )
    (csv_dir / "batch_002.csv").write_text(
        "name,mobile,reference,language\n"
        "Alice Updated,+91 9876543210,R1,hi\n",
        encoding="utf-8",
    )

    result = ingest_leads(tmp_path / "data", schema_root, pii_secret="test-secret")

    assert len(result.leads) == 1
    assert result.leads[0]["batch_id"] == "batch_001"
    assert result.leads[0]["phone_masked"] == "+91******3210"
    assert "phone" not in result.leads[0]
    assert "customer_name" not in result.leads[0]
    assert result.leads[0]["fields"]["language"] == "hi"
    assert result.metrics["lead_rows_total"] == 3
    assert result.metrics["lead_rows_rejected"] == 1
    assert result.metrics["duplicate_lead_rows_merged"] == 1
    assert result.rejections[0]["reason"] == "invalid_phone"
