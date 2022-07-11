The following syntax is supported:

* ``*`` - matches any number of any characters including none
* ``?`` - matches any single character
* ``[abc]`` - matches one character given in the bracket
* ``[a-z]`` - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ``!`` it unselects items that were selected by previous patterns
i.e. ``a?,!aa`` selects *ab* but not *aa*.
