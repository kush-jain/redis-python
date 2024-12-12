from app.rdb_parser import RDBParser


class TestRDBParser:

    def test_basic(self):
        """
        Sanity testing for RDB Parser
        """

        db = RDBParser("/opt/homebrew/var/db/redis", "dump.rdb")

        assert db.version == 3

        assert "redis-ver" in db.metadata
        assert "redis-bits" in db.metadata
        assert db.metadata["redis-bits"] in ("32", "64")
        assert "ctime" in db.metadata
        assert "used-mem" in db.metadata
