from entropylab.pipeline.params.persistence.tinydb_persistence import TinyDBPersistence


""" __generate_commit_id() """


def test__generate_commit_id():
    target = TinyDBPersistence()
    commit_id1 = target._TinyDBPersistence__generate_commit_id()
    commit_id2 = target._TinyDBPersistence__generate_commit_id()
    assert commit_id1 != commit_id2
