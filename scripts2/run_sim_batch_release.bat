RMDIR /S /Q configs

SET LOG="sim_batch_release.log"

IF EXIST %LOG% DEL %LOG%

sim_batch_release.bat >> %LOG%