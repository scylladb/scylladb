
.. caution::
   With :ref:`repair tombstone GC mode <ddl-tombstones-gc>`, window size must be configured carefully, for the table to not exceed the target of 20 windows within the repair interval.
       For example, if repair interval is usually 3 days, then picking a window size of 4h will ensure the number of windows will meet the target.