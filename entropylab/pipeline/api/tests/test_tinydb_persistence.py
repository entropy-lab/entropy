from entropylab.pipeline.params.persistence.tinydb.persistence import Persistence


""" __generate_commit_id() """


def test__generate_commit_id():
    target = Persistence()
    commit_id1 = target._TinyDBPersistence__generate_commit_id()
    commit_id2 = target._TinyDBPersistence__generate_commit_id()
    assert commit_id1 != commit_id2
