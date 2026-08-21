
---------------------------- MODULE compression ----------------------------

EXTENDS Integers, Sequences, TLC

CONSTANTS Dictionary, MaxNrUpdates

VARIABLES
senderRecentDict, senderCommittedDict, senderCurrentDict,
senderProtocolEpoch, senderHasUpdate, senderHasCommit,
receiverRecentDict, receiverCommittedDict, receiverCurrentDict,
receiverProtocolEpoch, receiverHasUpdate, receiverHasCommit,
senderToReceiver, receiverToSender, good,
nrUpdates

receiverVars == <<receiverRecentDict, receiverCommittedDict, receiverCurrentDict, receiverHasUpdate, receiverHasCommit, receiverProtocolEpoch>>
senderVars == <<senderRecentDict, senderCommittedDict, senderCurrentDict, senderHasUpdate, senderHasCommit, senderProtocolEpoch>>
vars == <<
    receiverRecentDict, receiverCommittedDict, receiverCurrentDict, receiverHasUpdate, receiverHasCommit, receiverProtocolEpoch,
    senderRecentDict, senderCommittedDict, senderCurrentDict, senderHasUpdate, senderHasCommit, senderProtocolEpoch,
    senderToReceiver, receiverToSender, good, nrUpdates>>

Init == \E x \in Dictionary : 
/\ senderRecentDict = x
/\ senderCommittedDict = x
/\ senderCurrentDict = x
/\ receiverRecentDict = x
/\ receiverCommittedDict = x
/\ receiverCurrentDict = x
/\ senderHasUpdate = FALSE
/\ receiverHasUpdate = FALSE
/\ senderHasCommit = FALSE
/\ receiverHasCommit = FALSE
/\ senderProtocolEpoch = 0
/\ receiverProtocolEpoch = 0
/\ senderToReceiver = <<>>
/\ receiverToSender = <<>>
/\ good = TRUE
/\ nrUpdates = 0

AnnounceDictionarySender(dict) ==
/\ senderRecentDict' = dict
/\ senderProtocolEpoch' = senderProtocolEpoch + 1
/\ senderHasUpdate' = TRUE
/\ senderHasCommit' = FALSE
/\ nrUpdates' = nrUpdates + 1
/\ nrUpdates < MaxNrUpdates
/\ UNCHANGED <<good, receiverVars, receiverToSender, senderToReceiver, senderCurrentDict, senderCommittedDict>>

AnnounceDictionaryReceiver(dict) ==
/\ receiverRecentDict' = dict
/\ receiverHasUpdate' = TRUE
/\ receiverHasCommit' = FALSE
/\ nrUpdates' = nrUpdates + 1
/\ nrUpdates < MaxNrUpdates
/\ UNCHANGED <<good, senderVars, receiverToSender, senderToReceiver, receiverCurrentDict, receiverCommittedDict, receiverProtocolEpoch>>

receiverDicts == <<receiverRecentDict, receiverCurrentDict, receiverCommittedDict>>
senderDicts == <<senderRecentDict, senderCurrentDict, senderCommittedDict>>

SenderSend ==
\/ /\ senderHasCommit
   /\ senderHasCommit' = FALSE
   /\ senderCurrentDict' = senderCommittedDict
   /\ senderToReceiver' = Append(senderToReceiver, <<"COMMIT", senderCommittedDict, senderProtocolEpoch>>)
   /\ UNCHANGED <<nrUpdates, good, receiverVars, receiverToSender, senderRecentDict, senderHasUpdate, senderCommittedDict, senderProtocolEpoch>>
\/ /\ senderHasUpdate
   /\ senderHasUpdate' = FALSE
   /\ senderCommittedDict' = senderRecentDict
   /\ senderToReceiver' = Append(senderToReceiver, <<"UPDATE", senderRecentDict, senderProtocolEpoch>>)
   /\ UNCHANGED <<nrUpdates, good, receiverVars, receiverToSender, senderHasCommit, senderRecentDict, senderCurrentDict, senderProtocolEpoch>>

ReceiverSend ==
\/ /\ receiverHasCommit
   /\ receiverHasCommit' = FALSE
   /\ receiverToSender' = Append(receiverToSender, <<"COMMIT", receiverCommittedDict, receiverProtocolEpoch>>)
   /\ UNCHANGED <<nrUpdates, good, senderVars, senderToReceiver, receiverHasUpdate, receiverProtocolEpoch, receiverDicts>>
\/ /\ receiverHasUpdate
   /\ receiverHasUpdate' = FALSE
   /\ receiverToSender' = Append(receiverToSender, <<"UPDATE", receiverRecentDict, receiverProtocolEpoch>>)
   /\ UNCHANGED <<nrUpdates, good, senderVars, senderToReceiver, receiverHasCommit, receiverProtocolEpoch, receiverDicts>>

Send ==
\/ ReceiverSend
\/ SenderSend

SenderRecv(msg) ==
\/ /\ msg[1] = "UPDATE"
   /\ senderProtocolEpoch' = senderProtocolEpoch + 1
   /\ senderHasUpdate' = TRUE
   /\ senderHasCommit' = FALSE
   /\ UNCHANGED <<nrUpdates, senderDicts>>
\/ /\ msg[1] = "COMMIT"
   /\ msg[3] = senderProtocolEpoch
   /\ senderCommittedDict' = IF msg[2] = senderCommittedDict THEN senderCommittedDict ELSE senderCurrentDict
   /\ senderHasCommit' = TRUE
   /\ UNCHANGED <<nrUpdates, senderProtocolEpoch, senderCurrentDict, senderRecentDict, senderHasUpdate>>
\/ /\ msg[1] = "COMMIT"
   /\ ~(msg[3] = senderProtocolEpoch)
   /\ UNCHANGED <<nrUpdates, senderVars>>

ReceiverRecv(msg) ==
\/ /\ msg[1] = "UPDATE"
   /\ receiverHasCommit' = TRUE
   /\ receiverHasUpdate' = FALSE
   /\ receiverCommittedDict' = IF msg[2] = receiverRecentDict THEN receiverRecentDict ELSE receiverCommittedDict
   /\ receiverProtocolEpoch' = msg[3] 
   /\ UNCHANGED <<nrUpdates, good, receiverCurrentDict, receiverRecentDict>>
\/ /\ msg[1] = "COMMIT"
   /\ good' = ((msg[2] = receiverCurrentDict) \/ (msg[2] = receiverCommittedDict))
   /\ receiverCurrentDict' = IF (msg[2] = receiverCommittedDict) THEN receiverCommittedDict ELSE receiverCurrentDict
   /\ UNCHANGED <<nrUpdates, receiverProtocolEpoch, receiverHasUpdate, receiverHasCommit, receiverCommittedDict, receiverRecentDict>>

Receive ==
\/ /\ Len(senderToReceiver) > 0
   /\ ReceiverRecv(Head(senderToReceiver))
   /\ senderToReceiver' = Tail(senderToReceiver)
   /\ UNCHANGED <<nrUpdates, receiverToSender, senderVars>>
\/ /\ Len(receiverToSender) > 0
   /\ SenderRecv(Head(receiverToSender))
   /\ receiverToSender' = Tail(receiverToSender)
   /\ UNCHANGED <<nrUpdates, good, senderToReceiver, receiverVars>>

Next ==
\/ \E dict \in Dictionary : AnnounceDictionarySender(dict)
\/ \E dict \in Dictionary : AnnounceDictionaryReceiver(dict)
\/ Send
\/ Receive

Spec == Init /\ [][Next]_vars
FairSpec == Init /\ [][Next]_vars /\ WF_vars(Send) /\ WF_vars(Receive)

AnnouncePossible == nrUpdates < MaxNrUpdates => \A d \in Dictionary: (ENABLED(AnnounceDictionaryReceiver(d)) /\ ENABLED(AnnounceDictionarySender(d)))
Good == good

Invariants ==
/\ ~(senderHasCommit /\ senderHasUpdate)
/\ ~(receiverHasCommit /\ receiverHasUpdate)
/\ Good
/\ AnnouncePossible

Settles == <>[](senderRecentDict = receiverRecentDict) ~> [](senderCurrentDict = senderRecentDict)

Properties ==
/\ Settles

=============================================================================
\* Modification History
\* Last modified Wed May 22 17:05:46 CEST 2024 by michal
\* Created Sat May 18 20:01:15 CEST 2024 by michal
