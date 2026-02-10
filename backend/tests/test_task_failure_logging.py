import os
import sys
import tempfile
import unittest
from pathlib import Path


class TestTaskFailureLogging(unittest.TestCase):
    def test_preprocess_failure_writes_task_failed_audit_log_with_redaction(self):
        from sqlalchemy.pool import StaticPool
        from sqlmodel import Session, SQLModel, create_engine, select
        from backend.app.models.audit import AuditLog
        from backend.app.models.task import DataTask
        import backend.app.api.task as task_api

        test_engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=False,
        )

        task_api.engine = test_engine

        SQLModel.metadata.create_all(test_engine)

        task = DataTask(name="sp12", task_type="sync_process", config="{}")
        with Session(test_engine) as session:
            session.add(task)
            session.commit()
            session.refresh(task)

        raw_error = "boom mysql+pymysql://user:secret@localhost:3306/test_db password=secret"

        def fake_submit(_task):
            return False, raw_error

        task_api.submit_spark_job = fake_submit

        task_api.run_spark_job_background(task.id)

        with Session(test_engine) as session:
            updated = session.get(DataTask, task.id)
            self.assertEqual(updated.status, "failed")

            logs = session.exec(
                select(AuditLog).where(AuditLog.resource == "sp12").order_by(AuditLog.id.desc())
            ).all()
            self.assertTrue(any(l.action == "task_failed" for l in logs))
            failed_log = next(l for l in logs if l.action == "task_failed")
            self.assertIn("boom", failed_log.details or "")
            self.assertNotIn("secret@localhost", failed_log.details or "")
            self.assertIn("****", failed_log.details or "")

    def test_pandas_preprocess_csv_roundtrip(self):
        repo_root = Path(__file__).resolve().parents[2]

        with tempfile.TemporaryDirectory() as tmp:
            prev_cwd = os.getcwd()
            sys_path_added = str(repo_root)
            try:
                os.chdir(tmp)
                sys.path.insert(0, sys_path_added)

                import pandas as pd
                from backend.spark_jobs.preprocess_job import run_pandas_job

                source_csv = Path(tmp) / "input.csv"
                out_dir = Path(tmp) / "out"

                pd.DataFrame(
                    [
                        {"a": 1, "b": None},
                        {"a": 1, "b": None},
                        {"a": 2, "b": "x"},
                    ]
                ).to_csv(source_csv, index=False)

                config = {
                    "source": {"type": "csv", "path": str(source_csv)},
                    "target": {"type": "csv", "path": str(out_dir), "mode": "overwrite"},
                    "operators": [{"type": "fill_na", "value": "0"}, {"type": "dedup"}],
                }

                run_pandas_job(config)

                output_csv = out_dir / "part-00000.csv"
                self.assertTrue(output_csv.exists())
                df_out = pd.read_csv(output_csv)
                self.assertEqual(len(df_out), 2)
            finally:
                os.chdir(prev_cwd)
                if sys.path and sys.path[0] == sys_path_added:
                    sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
