.. warning::

    If you are using materialized views or secondary indexes created in Scylla 2019.1.x and, **while** upgrading to 2020.1.x (7 or lower) updated your schema; you might have MV inconsistency.
    To fix: rebuild the MV.

    It is recommended to avoid schema and topology updates during upgrade (mix cluster).