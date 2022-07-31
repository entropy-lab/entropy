from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import (
    TinyDbPersistence,
)


""" __generate_commit_id() """


def test__generate_commit_id():
    target = TinyDbPersistence()
    commit_id1 = target._TinyDbPersistence__generate_commit_id()
    commit_id2 = target._TinyDbPersistence__generate_commit_id()
    assert commit_id1 != commit_id2
