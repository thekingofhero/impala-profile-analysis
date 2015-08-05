RMDIR /S /Q configs

SET LOG="sim_batch_debug.log"

IF EXIST %LOG% DEL %LOG%

sim_batch_debug.bat >> %LOG%