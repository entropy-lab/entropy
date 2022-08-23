# Alembic migrations used in Entropy ParamStore.

Entropy is using the open source [alembic project](https://alembic.sqlalchemy.org)
to manage structural changes to its internal databases. This includes the databases 
used by the *ParamStore*'s *SqlAlchemy Persistence* class which hold the contents of
the *ParamStore*.

The guidelines for using Alembic for managing the revisions (versions) of *ParamStore* 
databases are practically identical to those for the `SqlAlchemyDB` databases. Please
view the `README.md` at `entropylab/pipeline/results_backend/sqlalchemy/alembic` for 
further guidance.
