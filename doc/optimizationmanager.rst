Optimization manager
====================

Client commands:

- SUBMIT_BATCH
    - uid
    - submitter
    - description
    - data (array of (prod,CSV data))
    - strategy pickle
    - feed pickle
    - parameter batch (as per itertools output)
- BATCH_STATUS
    - uid
- REQUEST_RESULT
    - uid

Worker commands:

- REQUEST_JOB
- SUBMIT_RESULT
    - uid



.. automodule:: pyalgotrade.optimizer.optimizationmanager
    :members:
    :member-order: bysource
    :show-inheritance:


