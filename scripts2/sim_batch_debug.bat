@ECHO OFF

python config_gen.py config.template config.params

SET COF_DP_FILE="cofs_dp.csv"
SET LOG_LEVEL="off"

FOR /R configs %%G in (*.xml) do (	
	MOVE /Y "%%G" "%.\config.xml"	
	SET FileToDelete="%%~nG_debug.log"

	ECHO "==========================================================="
	ECHO "%%G"
	call :sim_exec_body	
)

:sim_exec_body
IF EXIST %FileToDelete% DEL %FileToDelete%		
SET STARTTIME=%time% 	
ImpalaFrameworkDebug.exe --cf-gui-connect=no --cf-gui-time-scale=ns "--cf-mon-on-time=0.0 us" "--cf-sim-duration=1 d" --cf-verbosity=%LOG_LEVEL% --cf-hpf-enable=no --cf-apf-enable=no --cf-gui-trace-enable=no --cf-lic-location=28518@plxs0415.pdx.intel.com --cf-log-file=%FileToDelete%  --cf-dp-values-file=%COF_DP_FILE%
SET ENDTIME=%time%

SET /A STARTTIME=(1%STARTTIME:~0,2%-100)*360000 + (1%STARTTIME:~3,2%-100)*6000 + (1%STARTTIME:~6,2%-100)*100 + (1%STARTTIME:~9,2%-100)
SET /A ENDTIME=(1%ENDTIME:~0,2%-100)*360000 + (1%ENDTIME:~3,2%-100)*6000 + (1%ENDTIME:~6,2%-100)*100 + (1%ENDTIME:~9,2%-100)
SET /A DURATION=%ENDTIME%-%STARTTIME%

ECHO "%STARTTIME%, %ENDTIME%, %DURATION%"
ECHO "==========================================================="

IF EXIST "config.xml" DEL "config.xml"