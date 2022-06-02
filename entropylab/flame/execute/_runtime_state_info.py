import os
import pathlib

import redis
import sqlalchemy

from sqlalchemy import text as sql_text

from entropylab.flame.execute._config import _Config, logger
from entropylab.flame.execute import _utils as execute_utils


def _setup_runtime_db(db_url, runtimedata_info):
    logger.debug("RuntimeStateInfo. Setup runtime database")
    with execute_utils.get_db_manager(db_url) as db:
        try:
            with db.begin():
                db.execute(sql_text("commit"))
                db.execute(
                    sql_text(
                        f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{runtimedata_info[3]}'
                AND pid <> pg_backend_pid();
                """
                    )
                )
                db.execute(sql_text(f"DROP DATABASE {runtimedata_info[3]}"))
                db.execute(sql_text("DROP OWNED BY workflow"))
                db.execute(sql_text("DROP USER workflow"))
        except Exception:
            # no such database
            logger.info("RuntimeStateInfo. No such database")
            pass

        with db.begin():
            db.execute(sql_text("commit"))
            db.execute(
                sql_text(f"CREATE DATABASE {runtimedata_info[3]} ENCODING 'UTF8'")
            )
            db.execute(sql_text(f"CREATE USER {runtimedata_info[0]}"))
            db.execute(
                sql_text(
                    f"ALTER USER {runtimedata_info[0]} PASSWORD '{runtimedata_info[1]}'"
                )
            )
            db.execute(
                sql_text(
                    f"""GRANT ALL PRIVILEGES ON DATABASE
                    {runtimedata_info[3]} TO {runtimedata_info[0]}"""
                )
            )


def _setup_runtime_state(host_url: str = "localhost", port=5431):
    logger.debug("RuntimeStateInfo. Setup runtime state")
    playbook_server = [host_url, 6379, 0]
    # db_user, db_password, db_URL, db_name
    runtimedata_info = ["workflow", "datapass", f"{host_url}:{port}", "workflow"]
    runtime_state = redis.Redis(
        host=playbook_server[0], port=playbook_server[1], db=playbook_server[2]
    )
    runtime_state.ping()
    return runtime_state, playbook_server, runtimedata_info


class RuntimeStateInfo:
    def __init__(self):
        rs, ps, rd_i = None, None, None
        try:
            rs, ps, rd_i = _setup_runtime_state("localhost", 5431)
            logger.info("RuntimeStateInfo. Host: localhost")
        except redis.ConnectionError:
            rs, ps, rd_i = _setup_runtime_state("runtimedata", 5432)
            logger.info("RuntimeStateInfo. Host: runtimedata")
        except Exception as e:
            logger.error(
                f"Unexpected exception: {str(e)}.\nTraceback: {e.__traceback__}.\n"
                f"To run Flame executor you have to have EntropyHub runtime layer "
                f"running on the computer. Please double check that this is not missing."
            )
        self.runtime_state = rs
        self.playbook_server = ps
        self.runtimedata_info = rd_i
        data_server_arg = f"{rd_i[0]},{rd_i[1]},{rd_i[2]},{rd_i[3]}"
        rs.set("dataserver", data_server_arg)
        _Config.DATABASE_NAME = (
            f"postgresql://{rd_i[0]}:{rd_i[1]}"
            f'@{rd_i[2]}/{rd_i[3]}?client_encoding="utf8"'
        )
        admin_password = os.getenv("POSTGRES_PASSWORD", "passwordpasswordpassword")
        _Config.SQLALCHEMY_DATABASE_URL = (
            f"postgresql://postgres:{admin_password}"
            f'@{rd_i[2]}/postgres?client_encoding="utf8"'
        )
        _setup_runtime_db(_Config.SQLALCHEMY_DATABASE_URL, rd_i)
        self.install_extensions()

    @staticmethod
    def install_extensions():
        logger.debug("RuntimeStateInfo. Install extensions")
        with execute_utils.get_runtimedata(_Config.DATABASE_NAME) as db:
            with db.begin():
                db.execute(sql_text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

                msgpack_sql_extension = os.path.join(
                    pathlib.Path(__file__).parent.resolve(), "./msgpack_decode.sql"
                )
                with open(msgpack_sql_extension) as file:
                    escaped_sql = sqlalchemy.text(file.read())
                    db.execute(escaped_sql)
