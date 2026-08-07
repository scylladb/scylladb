
How to Change Compaction Strategy
*********************************

This procedure describes how to change the compaction strategy.
A reason to change the compaction strategy can be performance tradeoffs (disk space usage, read and write amplification).
Change the compaction strategy means that the SSTables will be recompacted until the new compaction strategy is satisfied.
Changing the compaction strategy doesn't require a node to restart.

---------
Procedure
---------

1. Verify what the current compaction strategy is. Run the following:

.. code-block:: shell

   DESCRIBE TABLE nba.team_roster;

.. code-block:: shell

   DESCRIBE TABLE nba.team_roster ;

   CREATE TABLE nba.team_roster (
       player_name text PRIMARY KEY,
       player_jersy_number int,
       player_position text,
       team text
   ) AND compaction = {'class': 'SizeTieredCompactionStrategy'};
     
2. Change the compaction strategy to a new one. If you are unsure of which strategy to use, refer to :ref:`Choose a Compaction Strategy <which-strategy-is-best>` for more information. 

.. code-block:: shell

   ALTER TABLE nba.team_roster WITH compaction = {'class' :  'LeveledCompactionStrategy'}
