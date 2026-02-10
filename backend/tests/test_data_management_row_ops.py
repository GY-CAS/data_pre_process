import unittest

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine


class TestDataManagementRowOps(unittest.TestCase):
    def setUp(self):
        import backend.app.models.audit  # noqa: F401
        import backend.app.models.synced_table  # noqa: F401
        import backend.app.models.task  # noqa: F401
        import backend.app.models.datasource  # noqa: F401

        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=False,
        )
        SQLModel.metadata.create_all(self.engine)

        import backend.app.api.data_management as dm

        self.dm = dm
        self._orig_create_engine = dm.create_engine
        dm.create_engine = lambda *_args, **_kwargs: self.engine

    def tearDown(self):
        self.dm.create_engine = self._orig_create_engine

    def test_update_and_delete_row_with_pk(self):
        from sqlalchemy import text

        with self.engine.connect() as conn:
            conn.execute(text("CREATE TABLE `people` (`id` INTEGER PRIMARY KEY, `name` TEXT)"))
            conn.execute(text("INSERT INTO `people` (`id`, `name`) VALUES (1, 'a')"))
            conn.commit()

        with Session(self.engine) as session:
            res = self.dm.update_table_row(
                table_name="people",
                row_id="1",
                update=self.dm.RowUpdate(row_id="1", data={"name": "b"}),
                session=session,
            )
            self.assertEqual(res["ok"], True)

        with self.engine.connect() as conn:
            value = conn.execute(text("SELECT `name` FROM `people` WHERE `id`=1")).scalar()
            self.assertEqual(value, "b")

        with Session(self.engine) as session:
            res = self.dm.delete_table_row(table_name="people", row_id="1", session=session)
            self.assertEqual(res["ok"], True)

        with self.engine.connect() as conn:
            value = conn.execute(text("SELECT COUNT(*) FROM `people`")).scalar()
            self.assertEqual(value, 0)

    def test_update_requires_pk(self):
        from sqlalchemy import text
        from fastapi import HTTPException

        with self.engine.connect() as conn:
            conn.execute(text("CREATE TABLE `nopk` (`name` TEXT)"))
            conn.execute(text("INSERT INTO `nopk` (`name`) VALUES ('a')"))
            conn.commit()

        with Session(self.engine) as session:
            with self.assertRaises(HTTPException) as ctx:
                self.dm.update_table_row(
                    table_name="nopk",
                    row_id="1",
                    update=self.dm.RowUpdate(row_id="1", data={"name": "b"}),
                    session=session,
                )
            self.assertEqual(ctx.exception.status_code, 400)

    def test_update_and_delete_row_with_id_column_no_pk(self):
        from sqlalchemy import text

        with self.engine.connect() as conn:
            conn.execute(text("CREATE TABLE `t` (`id` INTEGER, `name` TEXT)"))
            conn.execute(text("INSERT INTO `t` (`id`, `name`) VALUES (1, 'a')"))
            conn.execute(text("INSERT INTO `t` (`id`, `name`) VALUES (2, 'c')"))
            conn.commit()

        with Session(self.engine) as session:
            res = self.dm.update_table_row(
                table_name="t",
                row_id="1",
                update=self.dm.RowUpdate(row_id="1", data={"name": "b"}),
                session=session,
            )
            self.assertEqual(res["ok"], True)

        with self.engine.connect() as conn:
            value = conn.execute(text("SELECT `name` FROM `t` WHERE `id`=1")).scalar()
            self.assertEqual(value, "b")

        with Session(self.engine) as session:
            res = self.dm.delete_table_row(table_name="t", row_id="2", session=session)
            self.assertEqual(res["ok"], True)

        with self.engine.connect() as conn:
            value = conn.execute(text("SELECT COUNT(*) FROM `t`")).scalar()
            self.assertEqual(value, 1)


if __name__ == "__main__":
    unittest.main()
